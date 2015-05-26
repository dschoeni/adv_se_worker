/**
 * Main application file
 */

'use strict';

// os utils
var os = require('os-utils');
var _ = require('lodash');
var mongoose = require('mongoose');
var async = require('async');
var http = require('http');

// socket.io
var io = require('socket.io')();
var redis = require('socket.io-redis');
io.adapter(redis({ host: 'localhost', port: 6379 }));

var socket = io.sockets;

socket.on('connect', function () { console.log("socket connected"); });
socket.on('connect-error', function(err) { console.log(err); });

// redis
var Queue = require('simple-redis-safe-work-queue');

var Sentiment = require('sentiment');

var keywordSchema = mongoose.Schema({
    keyword: String,
    tweetText: String,
    sentiment: Number,
    timestamp: Date
});
var Keyword = mongoose.model('Keyword', keywordSchema);

var statSchema = mongoose.Schema({
    instanceId: String,
    throughput: String,
    cpuload: Number,
    timestamp: Date
});
var WorkerStat = mongoose.model('Worker', statSchema);

// Connect to database
mongoose.connect('mongodb://localhost/advsetwitter-dev');

var processedTweets = 0;
var startTime = 0;
var worker = null;

// Get InstanceID from AWS
var instanceId = null;
http.get('http://169.254.169.254/latest/meta-data/instance-id', function (res) {
    var bodyarr = [];
    res.on('data', function (chunk) {
        bodyarr.push(chunk);
    }),
    res.on('end', function () {
        instanceId = bodyarr.join('').toString();
        console.log("Instance " + instanceId + " is up.")
        startWorker(); // start worker as soon as we have the instance id
    });
}).on('error', function(e) {
    console.log("Failed to get instanceId. Probably not an AWS Instance. Still starting...");
    instanceId = "no-aws-instance";
    startWorker(); // start worker even without instance id
});

// Worker function
var consumeTweet = function (tweet, callback) {

    if (tweet.entities == undefined || tweet == undefined) {
        callback(); // continue
        return;
    }

	processedTweets++;

    var phrases = tweet.phrase.split(",");
    var tweet_text = tweet.text;

    async.each(phrases, function (phrase, done) {

        var matched = _.find(tests, function (testfunction) {
            return testfunction(tweet, phrase);
        });

        if (matched) {
            Sentiment(tweet_text, function (err, result) {
                if (err) throw err;

                // var cleanedSentiment = (result.score + 5) / 10;

                var newKeyword = new Keyword({
                    keyword: phrase,
                    tweetText: tweet_text,
                    sentiment: result.score,
                    timestamp: new Date()
                });

                newKeyword.save(function (err, keyword) {
                    if (err) console.log(err);
                    done();
                });

            });
        } else {
            done();
        }

    }, function () {
        callback();
    });

};

var startWorker = function() {

	setInterval(function() {
		if (socket) {
			// update stats
			var timePassed = Date.now() - startTime;
			updateStats(timePassed, 'aws:update');

			// reset time for processing
			startTime = Date.now();
		}
	}, 1000);

    startTime = Date.now(); // starting time
    worker = Queue.worker('tweetQueue', consumeTweet);

};

var updateStats = function(timePassed, event) {

	var throughput = processedTweets / 5; // because we update every 5 seconds
	processedTweets = 0;

	os.cpuUsage(function (cpuload) {

        var stats = {
            instanceId: instanceId,
            throughput: Math.round(throughput),
            cpuload: Math.round(cpuload * 100),
            timestamp: new Date()
        }

        // save stats
        var worker = new WorkerStat(stats);
        worker.save(function (err, stats) {

            if (socket) {
                socket.emit(event, stats);
				console.log('emitted ' + event);
            }

            if (err) console.log(err);
            console.log(stats);

        });

    });
}

var aggregate = function () {
    var o = {};
    o.map = function () {
        emit(this.keyword, this.sentiment)
    };
    o.reduce = function (k, vals) {
        var sum = 0;
        for (var i = 0; i < vals.length; i++) {
            sum += vals[i];
        }
        return sum / vals.length;
    };
    o.out = {replace: 'aggregated'};
    o.verbose = true;

    Keyword.mapReduce(o, function (err, model, stats) {
        if (err) throw err;
        console.log('map reduce finished in ' + stats.processtime + ' ms. check collection aggregated!');
    })
};

function twitterMatch(text, phrase) {
    // TODO: to be improved with fancy regex according to https://dev.twitter.com/streaming/overview/request-parameters#track
    return text.toLowerCase().search(phrase.toLowerCase()) > -1;
}

var tests = [
    function testText(tweet, phrase) {
        return twitterMatch(tweet.text, phrase)
    },
    function testUrls(tweet, phrase) {
        return _.find(tweet.entities.urls, function (url) {
            return url.hasOwnProperty('expanded_url') && twitterMatch(url.expanded_url, phrase)
                || url.hasOwnProperty('display_url') && twitterMatch(url.display_url, phrase);
        });
    },
    function testMedia(tweet, phrase) {
        return _.find(tweet.entities.media, function (medium) {
            return medium.hasOwnProperty('expanded_url') && twitterMatch(medium.expanded_url, phrase)
                || medium.hasOwnProperty('display_url') && twitterMatch(medium.display_url, phrase);
        });
    },
    function testHashtags(tweet, phrase) {
        return _.find(tweet.entities.hashtags, function (hashtag) {
            return hashtag.hasOwnProperty('text') && twitterMatch(hashtag.text, phrase)
        });
    },
    function testUserMentions(tweet, phrase) {
        return _.find(tweet.entities.user_mentions, function (user) {
            return user.hasOwnProperty('screen_name') && twitterMatch(user.screen_name, phrase)
        });
    }
];