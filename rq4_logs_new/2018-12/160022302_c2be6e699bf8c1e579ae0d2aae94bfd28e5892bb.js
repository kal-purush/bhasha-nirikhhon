const steem = require('steem');

// var seedrandom = require('seedrandom');

const {createServer} = require('http').createServer().listen(3000)

//CONSTANTS AND VARIABLES

      var steemStream;

      var check = 1;

      const ACCOUNT_NAME = 'eosbpnews';

      const ACCOUNT_KEY = process.env.STEEMPOSTINGKEY;
/****************************************************************************************************************************************************/

const MESSAGE='I just resteemed your post!<br><br> Why? @eosbpnews aggregates updates of active EOS BPs and conviniently serves them in one place!<br><hr><sub><i>This service is provided by @eosoceania. If you think we are doing useful work, consider supporting us with a vote :) <br>For any inquiries/issues please reach out on <a href="https://t.me/joinchat/IB6xJg7tmo7v4knEJyQRSw">Telegram</a> or <a href="https://discord.gg/eAdBZBv">Discord</a>.</i></sub>';

// BLACKLIST
var blacklist = ['mutiarahmi','srimulyani','adam.smit'];

// WHITELIST
var whitelist = ['eosoceania','questingtw'];
//START

      console.log('Bot started. Checking transactions, listening to Block Producers... ');

      steem.api.setOptions({ url: 'https://api.steemit.com' });

//TIMER

      setTimeout(function(){

        // start stream after 5 seconds

        startSteemStream()

      },5000)

//GET DATA FROM BLOCKCHAIN

function startSteemStream(){

steemStream = steem.api.streamTransactions('head', function(err, result) {

try{

    var txType = result.operations[0][0]

    var txData = result.operations[0][1]

//Check that it is a post

      if (txType=='comment' && (txData.parent_author=="")){

          var author = txData.author;

          var link = txData.permlink;

          //console.log('processing post by: ', author, ' link: ', link);

					var json;

          var problem;

            json = JSON.parse(txData.json_metadata);

                      // if(json.hasOwnProperty('tags')){
                      //
                      //     hasTag=json.tags.indexOf(TAG);
                      //
                      //         if(hasTag > -1){

                                    if(blacklist.indexOf(author) > -1){

                                      console.log(author, ': This User is blacklisted.');

                                      // postWarning(ACCOUNT_NAME,ACCOUNT_KEY,author,link);

                                                    }

                                    else if(whitelist.indexOf(author) > -1){

                                            console.log('Tag found in: ',link, ' by: ', author);

                                            sendResteem(ACCOUNT_NAME, ACCOUNT_KEY, author, link);
                                            postComment(ACCOUNT_NAME,ACCOUNT_KEY,author,link);

                                                  }//close else if

                                    else{

                                      console.log('User not on list, post ignored.');

                                    }

                                    //         } // 1. close if hasTag
                                    //
                                    // }// 3. close hasOwnProperty

                        }//close if=comment

              }//TRY

              catch(error){

                console.log(error)

                // if error restart stream

                restartSteemStream()

                            } //catch

        }//close err funk

    );//close streamTransactions

}//steemStream

function endSteemStream(){

  steemStream()

}

function restartSteemStream(){

  endSteemStream()

  startSteemStream()

}

        function sendVote(ACCOUNT_KEY, ACCOUNT_NAME,author,link, weight){

            steem.broadcast.vote(ACCOUNT_KEY, ACCOUNT_NAME, author, link, weight, function(err, result) {

                console.log(err, result);

                console.log('Voted on post: ' ,link, ' by: ', author );

            });

        }


function sendResteem(acc,wif,username,link){

  const jsonObj = JSON.stringify(['reblog', {
    account: acc,
    author: username,
    permlink: link
  }]);

  steem.broadcast.customJson(wif, [], [acc], 'follow', jsonObj, (err, result) => {
    console.log(err, result);
    console.log('resteemed!');
  });
}
        function postWarning(ACCOUNT_NAME,ACCOUNT_KEY,author,link){

        var permlink = new Date().toISOString().replace(/[^a-zA-Z0-9]+/g, '').toLowerCase();

        steem.broadcast.comment(

          ACCOUNT_KEY,

          author, // Parent Author

          link, // Parent Permlink

          ACCOUNT_NAME, // Author

          permlink, // Permlink

          '', // Title

          '<h2>We do not encourage plagiarism, spam or tag abuse. This user has been blacklisted!</h2>', // Body,

          { tags: ['test'], app: 'steemjs' }, // Json Metadata

          function(err, result) {

            console.log(err, result);

            console.log('Commented on post.');

                                }

                        );

        }

        function postComment(ACCOUNT_NAME,ACCOUNT_KEY,author,link){

        var permlink = new Date().toISOString().replace(/[^a-zA-Z0-9]+/g, '').toLowerCase();

	         dropstring=MESSAGE;

        steem.api.getContentReplies(author, link, function(err, result) {

        check = 1;

        if(result.length>0){

          for(i=0;i<result.length;i++){

            if(result[i].author==ACCOUNT_NAME){

              check = 0;

                }

              }

            }

            if(check==1){

              	steem.broadcast.comment(

                ACCOUNT_KEY,

                author, // Parent Author

                link, // Parent Permlink

                ACCOUNT_NAME, // Author

                permlink, // Permlink

                '', // Title

                dropstring, // Body,

                { tags: ['kiwi'], app: 'steemjs' }, // Json Metadata

                function(err, result) {

                  console.log(err, result);

                  console.log('Commented on post.');

                                      }

                              );

                    }

            else {

              console.log('Commented on this post already!')

                }

          });

}