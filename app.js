/**
 * Main application file
 */

'use strict';

// os utils
var os = require('os-utils');
var _ = require('lodash');
var mongoose = require('mongoose');
var async = require('async');

// redis
var Queue = require('simple-redis-safe-work-queue');

var Sentiment = require('sentiment');

var keywordSchema = mongoose.Schema({
	keyword: String,
	sentiment: Number,
	timestamp: Date
});

var Keyword = mongoose.model('Keyword', keywordSchema);

// Connect to database
mongoose.connect('mongodb://localhost/keywords-test');

var totalTweets = 0;

// Worker function
var consumeTweet = function (tweet, callback) {

	if (totalTweets % 500 == 0) {
		aggregate();
	}

	if (tweet.entities == undefined || tweet == undefined) {
		callback(); // continue
		return;
	}

	totalTweets++;

	if (totalTweets % 50 == 0) {
		os.cpuUsage(function (v) {
			console.log('CPU Usage (%): ' + v);
		});
	}

	var phrases = tweet.phrase.split(",");
	var tweet_text = tweet.text;

	async.each(phrases, function (phrase, done) {

		var matched = _.find(tests, function (testfunction) {
			return testfunction(tweet, phrase);
		});

		if (matched) {
			Sentiment(tweet_text, function (err, result) {
				if (err) throw err;

				var newKeyword = new Keyword({
					keyword: phrase,
					sentiment: result.score,
					timestamp: new Date()
				});

				newKeyword.save(function (err, keyword) {
					if (err) {
						console.log(err);
					} else {
						done();
					}
				});
			});
		} else {
			done();
		}

	}, function () {
		callback();
	});

};

var worker = Queue.worker('tweetQueue', consumeTweet);

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
	return text.search(phrase) > -1;
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