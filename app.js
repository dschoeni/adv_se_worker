/**
 * Main application file
 */

'use strict';

// os utils
var os = require('os-utils');
var _ = require('lodash');

// redis
var queueName = "tweetQueue";
var chnl = require('node-redis-queue').Channel;
var channel = new chnl();
var Sentiment = require('sentiment');

// mongodb
var MongoClient = require('mongodb').MongoClient;
var url = 'mongodb://localhost:27017/myproject';
var collection;

// Use connect method to connect to the Server
MongoClient.connect(url, function (err, db) {
    console.log("Connected correctly to server");
    collection = db.collection('sentiment');

    channel.on('error', function (error) {
        console.log('Stopping due to: ' + error);
        process.exit();
    });

    channel.connect(function () {
        console.log('ready');
        consumeTweet(); // enter consume-loop
    });

});

// some vars we need
var tweetsCount = 0;
var tweetsTotalSentiment = 0;

// Worker funktion
var consumeTweet = function () {

    channel.pop(queueName, function (tweet) {

        var phrases = tweet.phrase.split(",");
        var tweetText = tweet.text + tweet.username + tweet.hashtag;

        _.forEach(phrases, function (phrase) {
            if (tweetText.search(phrase) !== -1) {

                Sentiment(tweet.text, function (err, result) {
                    tweetsTotalSentiment += result.score;
                    tweetsCount++;

                    if (tweetsCount % 15 === 0) {
                        var calculatedSentiment = tweetsTotalSentiment / 15;
                        tweetsTotalSentiment = 0;

                        os.cpuUsage(function (v) {
                            console.log('CPU Usage (%): ' + v);
                        });

                        collection.insertOne({
                            phrase: phrase,
                            sentiment: calculatedSentiment
                        }, function (err, result) {
                            console.log("Inserted Tweets");
                        });

                        consumeTweet(); // and consume moar!

                    } else {
                        consumeTweet(); // consume moar!
                    }

                });

            } else {
                console.log("Skipped Tweet.");
            }
        });

    });
}