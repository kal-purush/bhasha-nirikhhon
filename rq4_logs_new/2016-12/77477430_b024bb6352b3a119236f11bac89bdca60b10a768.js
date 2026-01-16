var Botkit = require('botkit');
var os = require('os');
var pg = require('pg');

// create a config to configure both pooling behavior
// and client options
// note: all config is optional and the environment variables
// will be read if the config is not present
var config = {
    user: 'postgres', //env var: PGUSER
    database: 'AptitivePG', //env var: PGDATABASE
    password: 'aptitive', //env var: PGPASSWORD
    host: '192.168.2.169', // Server hosting the postgres database
    port: 5432, //env var: PGPORT
    max: 10, // max number of clients in the pool
    idleTimeoutMillis: 30000,  // how long a client is allowed to remain idle before being closed
};

var controller = Botkit.slackbot({
    debug: false
    //include "log: false" to disable logging 
    //or a "logLevel" integer from 0 to 7 to adjust logging verbosity 
});




// connect the bot to a stream of messages 
controller.spawn({
    token: 'xoxb-75882476483-6Pm84qnzplVWFmvhdRvKPZ2W',
}).startRTM()


controller.hears(['hello', 'hi', 'start', 'wakeup'], 'direct_message, direct_mention, mention',
    function (bot, message) {

        bot.startConversation(message, function (err, convo) {

            convo.ask('Are you ready to give some feedback?', [
                {
                    pattern: bot.utterances.no,
                    callback: function (response, convo) {
                        convo.say('Perhaps later.');
                        convo.next();
                    }
                },
                {
                    default: true,
                    callback: function (response, convo) {
                        // just repeat the question
                        convo.repeat();
                        convo.next();
                    }
                },
                {
                    pattern: bot.utterances.yes,
                    callback: function (response, convo) {
                        convo.say('Great! Let\'s do this. You can tell me you are \'Done\' at anytime.');

                        convo.ask('Choose one of the following feedback types: >, <, ++, +, -, --', [
                            {
                                default: true,
                                callback: function (response, convo) {
                                    // just repeat the question
                                    convo.repeat();
                                    convo.next();
                                }
                            },
                            {
                                pattern: ['done', 'Done'],
                                callback: function (response, convo) {
                                    convo.exit();
                                }
                            },
                            {
                                pattern: '>',
                                callback: function (response, convo) {
                                    submitFeedback('>', response.user, response.text);
                                    convo.next();
                                }
                            },
                        ]);
                        convo.next();
                    }
                }
            ]);
        });

        //bot.api.reactions.add({
        //    timestamp: message.ts,
        //    channel: message.channel,
        //    name: 'robot_face',
        //}, function (err, res) {
        //    if (err) {
        //        bot.botkit.log('Failed to add emoji reaction :(', err);
        //    }
        //        });


        bot.reply(message, 'Noted');

    });



controller.hears('yo', ['direct_mention', 'mention', 'direct_message'], function (bot, message) {

    // start a conversation to handle this response.
    bot.startConversation(message, function (err, convo) {

        convo.ask('What\'s up dog? You ready to give some feedback?', function (response, convo) {

            convo.say('Cool, you said: ' + response.text);
            convo.next();
        });

        bot.say(
            {
                text: 'Party people - someone is using your bot in another channel...',
                channel: 'G272A357T', // a valid slack channel, group, mpim, or im ID
            }
        );

    })


});


controller.hears(['uptime', 'identify yourself', 'who are you', 'what is your name'],
    'direct_message,direct_mention,mention', function (bot, message) {

        var hostname = os.hostname();
        var uptime = formatUptime(process.uptime());

        bot.reply(message,
            ':robot_face: I am a bot named <@' + bot.identity.name +
            '>. I have been running for ' + uptime + ' on ' + hostname + '.');

    });


controller.hears(['shutdown'], 'direct_message,direct_mention,mention', function (bot, message) {

    bot.startConversation(message, function (err, convo) {

        convo.ask('Are you sure you want me to shutdown?', [
            {
                pattern: bot.utterances.yes,
                callback: function (response, convo) {
                    convo.say('Ok... See you later I guess.');
                    convo.next();
                    setTimeout(function () {
                        process.exit();
                    }, 3000);
                }
            },
            {
                pattern: bot.utterances.no,
                default: true,
                callback: function (response, convo) {
                    convo.say('*Phew!*');
                    convo.next();
                }
            }
        ]);
    });
});



function submitFeedback(score, messageuser, messagetext) {

    //this initializes a connection pool
    //it will keep idle connections open for 5 minutes
    //and set a limit of maximum 10 idle clients
    var pool = new pg.Pool(config);

    // to run a query we can acquire a client from the pool,
    // run a query on the client, and then return the client to the pool
    pool.connect(function (err, client, done) {
        if (err) {
            return console.error('error fetching client from pool', err);
        }

        client.query('INSERT INTO slack.feedbacklog (providername, score, recorddate, feedbacktext) VALUES(\''
            + messageuser + '\', \'' + score + '\', CURRENT_TIMESTAMP,\'' + messagetext + '\');',
            function (err, result) {
                //call `done()` to release the client back to the pool
                done();

                if (err) {
                    return console.error('error running query', err);
                }
            });

    });

    pool.on('error', function (err, client) {
        // if an error is encountered by a client while it sits idle in the pool
        // the pool itself will emit an error event with both the error and
        // the client which emitted the original error
        // this is a rare occurrence but can happen if there is a network partition
        // between your application and the database, the database restarts, etc.
        // and so you might want to handle it and at least log it out
        console.error('idle client error', err.message, err.stack)
    })
}

function formatUptime(uptime) {
    var unit = 'second';
    if (uptime > 60) {
        uptime = uptime / 60;
        unit = 'minute';
    }
    if (uptime > 60) {
        uptime = uptime / 60;
        unit = 'hour';
    }
    if (uptime != 1) {
        unit = unit + 's';
    }

    uptime = uptime + ' ' + unit;
    return uptime;
}