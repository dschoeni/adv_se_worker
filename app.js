/**
 * Main application file
 */

'use strict';

// os utils
var os = require('os-utils');
var _ = require('lodash');
var mongoose = require('mongoose');


// redis
var queueName = "tweetQueue";
var chnl = require('node-redis-queue').Channel;
var channel = new chnl();
var Sentiment = require('sentiment');

var keywordSchema = mongoose.Schema({
    keyword: String,
    sentiment: Number,
    timestamp: Date
});

var Keyword = mongoose.model('Keyword', keywordSchema);

// Connect to database
mongoose.connect('mongodb://localhost/keywords-test');

channel.on('error', function (error) {
    console.log('Stopping due to: ' + error);
    process.exit();
});

channel.connect(function () {
    console.log('ready');
    consumeTweet(); // enter consume-loop
});

var totalTweets = 0;

// Worker function
var consumeTweet = function () {

    if (totalTweets % 500 == 0) {
        aggregate();
    }

    channel.pop(queueName, function (tweet) {

        if (tweet.entities == undefined) {
            consumeTweet();
            return;
        }

        totalTweets++;

        if (totalTweets % 15 == 0) {
            os.cpuUsage(function (v) {
                console.log('CPU Usage (%): ' + v);
            });
        }

        var phrases = tweet.phrase.split(",");
        var tweet_text = tweet.text;
        var urls = tweet.entities.urls; // [] -> expanded_url, display_url
        var media = tweet.entities.media; // [] -> expanded_url, display_url
        var hashtags = tweet.entities.hashtags; // [] -> text
        var user_mentions = tweet.entities.user_mentions; // [] -> screen_name

        var match = function (phrase) {

            var match = false;

            var twitterMatch = function (text) {
                // TODO: to be improved with fancy regex according to https://dev.twitter.com/streaming/overview/request-parameters#track
                return text.search(phrase) > -1;
            };

            // Check Tweet text
            if (twitterMatch(tweet_text)) {
                // console.log("Matched " + phrase + " in text: " + tweet_text);
                match = true;
            }

            // check urls
            _.forEach(urls, function (url) {
                if (url.hasOwnProperty('expanded_url') && twitterMatch(url.expanded_url)
                    || url.hasOwnProperty('display_url') && twitterMatch(url.display_url)) {
                    // console.log("Matched " + phrase + " in URL:");
                    // console.log(url);
                    match = true;
                }
            });

            // check media
            _.forEach(media, function (medium) {
                if (medium.hasOwnProperty('expanded_url') && twitterMatch(medium.expanded_url)
                    || medium.hasOwnProperty('display_url') && twitterMatch(medium.display_url)) {
                    // console.log("Matched " + phrase + " in media:");
                    // console.log(medium);
                    match = true;
                }
            });

            // check hashtags
            _.forEach(hashtags, function (ht) {
                if (ht.hasOwnProperty('text') && twitterMatch(ht.text)) {
                    // console.log("Matched " + phrase + " in hashtag: " + ht.text);
                    match = true;
                }
            });

            // check user mentions
            _.forEach(user_mentions, function (user) {
                if (user.hasOwnProperty('screen_name') && twitterMatch(user.screen_name)) {
                    //console.log("Matched " + phrase + " in user mentions: " + user.screen_name);
                    match = true;
                }
            });

            return match;
        };

        _.forEach(phrases, function (phrase) {
            var matched = match(phrase);
            // console.log(matched);
            if (matched) {

                Sentiment(tweet_text, function (err, result) {
                    if (err) throw err;

                    var newKeyword = new Keyword({
                        keyword: phrase,
                        sentiment: result.score,
                        timestamp: new Date()
                    });

                    newKeyword.save(function (err, keyword) {
                        if (err) throw err;
                        // console.log("Inserted new phrase");
                        consumeTweet();
                    });
                });
            } else {
                consumeTweet();
            }
        });
    });
};

var aggregate = function() {
    var o = {};
    o.map = function() { emit(this.keyword, this.sentiment) };
    o.reduce = function(k, vals) {
        var sum = 0;
        for (var i = 0; i < vals.length; i++) {
            sum += vals[i];
        }
        return sum / vals.length;
    };
    o.out = { replace: 'aggregated'};
    o.verbose = true;

    Keyword.mapReduce(o, function(err, model, stats) {
        if (err) throw err;
        console.log('map reduce finished in ' + stats.processtime + ' ms. check collection aggregated!');
    })
};