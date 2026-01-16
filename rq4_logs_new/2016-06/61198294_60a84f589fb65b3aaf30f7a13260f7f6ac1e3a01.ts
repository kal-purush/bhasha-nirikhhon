const request = require('request');
var fs = require('fs');

exports = module.exports = function(app) {
  app.get('/api/sed/', function(req, res) {
    var options = {
      url: 'https://api.voicetext.jp/v1/tts',
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded'
      },
      auth: {
        user: 'urcoqo00kebdpb4p',
        password: ''
      },
      form: req.query,
      encoding: null
    };
    console.log(options);
    request(options, (error, response, body) => {
      if (!error && response.statusCode == 200) {
        var binaly = body;
        console.log(new Buffer(binaly));
        var fileName = (new Date()).getTime() + '.wav';
        var path = `src/` + fileName;
        fs.writeFile(path, binaly, err => {
          console.log(err);
          res.send(fileName);}
      );
      }else {
        console.log('error: '+ response.statusCode);
        console.log(response);
      }
    })
  });
};