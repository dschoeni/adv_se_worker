/**
 * Main application file
 */

'use strict';

// env
var streamerHost = process.env.STREAMER_HOST || 'localhost:9000';
var mongoDbUrl = process.env.MONGO_URL || 'mongodb://localhost/advsetwitter-dev';
var redisPort = process.env.REDIS_PORT || '6379';
var redisHost = process.env.REDIS_HOST || 'localhost';


// os utils
var os = require('os-utils');
var _ = require('lodash');
var mongoose = require('mongoose');
var async = require('async');
var request = require('request');

// socket.io server to emit to all clients
var io = require('socket.io')();
var redis = require('socket.io-redis');
io.adapter(redis({
	host: redisHost,
	port: redisPort
}));

// socket.io client
var socketioclient = require('socket.io-client');
var socket = socketioclient.connect('ws://' + streamerHost, {path: '/socket.io-client', transports: ['websocket']});

socket.on('connect_error', function (data) {
    console.log("websocket couldn't make connection.")
});

socket.on('scalefactor:update', function (data) {
	scaleFactor = data.scaleFactor * 100; // slow worker down considerably
	console.log('new scalefactor: ' + scaleFactor);
});

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
mongoose.connect(mongoDbUrl);

var processedTweets = 0;
var startTime = 0;
var worker = null;

// Worker ScaleFactor
var instanceId = "no-aws-instance";
var scaleFactor = 1;

request({ url: 'http://169.254.169.254/latest/meta-data/instance-id', timeout: 2000 }, function (error, response, body) {

	if (error) {
		console.log("no aws instance. use local from env: " + process.env.INSTANCEID);
		instanceId = process.env.INSTANCEID;
	}

	if (!error && response.statusCode == 200) {
		instanceId = body;
	}

    request({ url: 'http://' + streamerHost + '/api/aws/scalefactor', timeout: 2000 }, function (error, response, body) {

		if (error) {
			console.log("couldn't get scalefactor: " + error.toString());
		}

		if (!error && response.statusCode == 200) {
			scaleFactor = JSON.parse(body).scaleFactor;
			console.log("scalefactor available: " + scaleFactor);
		}

		startWorker();

	});

});

// Worker function
var consumeTweet = function (tweet, callback) {

	processedTweets++;
	if (tweet.entities == undefined || tweet == undefined) {
		callback(); // continue
		return;
	}

	var phrases = tweet.phrase.split(",");
	var tweet_text = tweet.text;

	async.each(phrases, function (phrase, done) {

		var i = 0;
		var matched;

		for (i = 0; i <= scaleFactor; i++) {

			var matched = _.find(tests, function (testfunction) {
				return testfunction(tweet, phrase);
			});

		}

		if (matched) {
			for (i = 0; i <= scaleFactor; i++) {

				if (i == scaleFactor) {
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
					Sentiment(tweet_text, function (err, result) {

					});
				}

			}
		} else {
			done();
		}

	}, function () {

		callback();
	});

};

var startWorker = function () {

	setInterval(function () {
		sendStatus();
	}, 5000);

	startTime = Date.now(); // starting time
	sendStatus();

	worker = Queue.worker('tweetQueue', consumeTweet, {port: redisPort, host: redisHost});

};

var sendStatus = function () {
	if (io.sockets) {
		// update stats
		var timePassed = Date.now() - startTime;
		updateStats(timePassed, 'aws:update');

		// reset time for processing
		startTime = Date.now();
	}
}

var updateStats = function (timePassed, event) {

	var throughput = processedTweets / (timePassed / 1000); // because we update every 5 seconds
	processedTweets = 0;

	os.cpuUsage(function (cpuload) {

		var stats = {
			instanceId: instanceId,
			throughput: Math.round(throughput),
			cpuload: Math.round(cpuload * 100),
			timestamp: new Date()
		}

		if (io.sockets) {
			io.sockets.emit(event, stats);
		}

		/*
		 // save stats
		 var worker = new WorkerStat(stats);
		 worker.save(function (err, stats) {

		 if (io.sockets) {
		 io.sockets.emit(event, stats);
		 }

		 if (err) console.log(err);
		 console.log("Speed: " + stats.throughput);

		 });
		 */

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