'use strict';

var express = require('express'); // app server
var bodyParser = require('body-parser'); // parser for post requests
var Conversation = require('watson-developer-cloud/conversation/v1'); // conversation middleware
var agent = require('bluemix-autoscaling-agent'); // autoscaling middleware
//var XMLHttpRequest = require("xmlhttprequest").XMLHttpRequest; // xhr의 요청은 loding으로 인한 예외발생
//var xhr = new XMLHttpRequest();
var request = require('request-promise');

var app = express();
app.use(bodyParser.json());


/* watson conversation 생성 */
var conversation = new Conversation({
  // If unspecified here, the CONVERSATION_USERNAME and CONVERSATION_PASSWORD env properties will be checked
  // After that, the SDK will fall back to the bluemix-provided VCAP_SERVICES environment property
  username: 'ca3d49c3-1d6e-4d9d-a1b1-7b65c7119469', //Conversation 서비스 신임정보의 username, password
  password: '6LPA0DnoXqkR',                         //process.env을 사용하셔도 됩니다
  url: 'https://gateway.watsonplatform.net/conversation/api',
  version_date: Conversation.VERSION_DATE_2017_04_21 // latest version 2017_05_26
});
/* end of watson conversation 생성 */

// kakao init
app.get('/keyboard',function(req,res){
  var keyboard={
    "type" : "text" // or button
  };
  res.send(keyboard);
});

// kakao message
app.post('/message',function(req,res){
  var content = decodeURIComponent(req.body.content); // recieve text from user
  var user_key = decodeURIComponent(req.body.user_key); // user's key

  // workspace id check
  var workspace = process.env.WORKSPACE_ID || '11298b60-486d-4dad-961a-d2537f8e6070';

  if (!workspace || workspace === '<workspace_id>') {
    return res.json({
      'message': {
        'text': '유효하지 않은 워크스페이스 아이디입니다.'
      }
    });
  }

  // watson 내 DB구축 제한 사항이 있어 외부 DB Server 구축하여 api로 작업했습니다
  // user_key, context 필드를 참조하여 필요한 정보 가져오기, 갱신합니다
  // 아래 코드는 프로토타입으로 작성한 코드이므로 논리적 결함이 있을 수 있으니
  // 사용중인 DB로 로직 작성하시거나 api부분을 대체하시면 됩니다
  var reqOptions = {
    method: 'GET',
    uri: 'http://125.180.251.251:3000/wcs/'+user_key+'/getContext',
    qs:{},
    headers: {
      'User-Agent': 'Request-Promise'
    },
    json: true
  };

  request(reqOptions) // user_key에 해당하는 context 정보를 가져옴
    .then(function(response){
      var context=response;

      var payload = { // payload init
        workspace_id: workspace,
        context: context,
        input: {"text":content}
      };

      var sendMessage = new Promise((resolve, reject)=>
        conversation.message(payload, function(err, data) {
          if (err) {
            reject(err);
          }else{
            resolve(data);
          }
        })
      );

      sendMessage.then(wcs_response=>{
        var output = wcs_response.output.text[0]; // output.text는 배열
        var new_context = wcs_response.context; // wcs 응답 context
        var message={
          "message":{
            "text" : output
          }
        };

        // update context
        var reqOptions = {
          method: 'GET',
          uri: 'http://125.180.251.251:3000/wcs/'+user_key+'/'+JSON.stringify(new_context),
          headers: {
            'User-Agent': 'Request-Promise'
          },
          json: true
        };
        request(reqOptions) // user_key에 해당하는 context 정보를 가져옴
          .then(function(response){
            if(response){
                res.send(message); // context 갱신 성공 시 kakao에 답변 전달
            }
          }).catch(function(err){ // 외부DB서버 api 요청 에러
            var message={
              "message":{
                "text" : "api error : "+err
              }
            };
            res.send(message);
          });
      });
    }).catch(function(err){ // 외부DB서버 api 요청 에러
      if(err){
        var message={
          "message":{
            "text" : "api error : "+err
          }
        };
        res.send(message);
      }
    });

/*
  xhr.open("GET",'http://125.180.251.251:3000/wcs/'+user_key+'/getContext', true);
  xhr.onreadystatechange=function(){
  if (xhr.readyState==4 && xhr.status==200){
    var context=JSON.parse(xhr.responseText);
  //console.log(context);

    var payload = { // make payload
      workspace_id: workspace,
      context: {"context":context},
      input: {"text":content}
    };

    var sendMessage = new Promise((resolve, reject)=>
      conversation.message(payload, function(err, data) {
        if (err) {
          reject(err);
        }else{
          resolve(data);
        }
      })
    );

    sendMessage.then(response=>{
      var output = response.output.text[0];
      var message={
        "message":{
          "text" : output
        }
      };
    res.send(message);
    });
    //res.json(context);
  }else{
    var message={
      "message":{
        "text" : "xhr error "+xhr.readyState+' '+xhr.status
      }
    };
    res.send(message);
  }
}
xhr.send();
*/

/*
  var payload = { // make payload
    workspace_id: workspace,
    context: req.body.context || {},
    input: {"text":content}
  };
*/
});

app.get('/friend',function(req,res){

});



module.exports = app;