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

// moving average util
var movingAverage = require('moving-average');
var ma = movingAverage(45 * 1000);

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
	scaleFactor = data.scaleFactor;
	if (scaleFactor > 1) { // slow worker down considerably
		//scaleFactor = scaleFactor * 20;
	}
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
			if (scaleFactor > 1) { // slow worker down considerably
				//scaleFactor = scaleFactor * 20;
			}
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

		var matched;

		for (var i = 0; i <= scaleFactor; i++) {
			var matched = _.find(tests, function (testfunction) {
				return testfunction(tweet, phrase);
			});
		}

		if (matched) {
			for (var u = 0; u <= scaleFactor; u++) {

				if (u == scaleFactor) {
					Sentiment(tweet_text, function (err, result) {
						if (err) throw err;

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

	startTime = Date.now(); // starting time

	setInterval(function () {
		sendStatus();
	}, 5000);

	sendStatus();

	worker = Queue.worker('tweetQueue', consumeTweet, {port: redisPort, host: redisHost});
	console.log("Starting to process tweets...");

};

var sendStatus = function () {
	if (io.sockets) {
		// update stats
		updateStats('aws:update');
	}
}

var updateStats = function (event) {

	var currentThroughput = processedTweets / ((Date.now() - startTime) / 1000) || 0;
	ma.push(Date.now(), currentThroughput);
	processedTweets = 0;
	startTime = Date.now();
	var movingAverage = ma.movingAverage();

	os.cpuUsage(function (cpuload) {

		var stats = {
			instanceId: instanceId,
			throughput: Math.round(movingAverage),
			cpuload: Math.round(cpuload * 100),
			timestamp: new Date()
		}

		if (io.sockets) {
			io.sockets.emit(event, stats);
		}

	});
}

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