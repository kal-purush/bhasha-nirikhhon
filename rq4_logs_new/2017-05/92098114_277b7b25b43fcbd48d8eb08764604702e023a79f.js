var urlopen = require('urlopen');

var options = {
            target: 'http://dpanda.localhost:65012/default-log-xml',
            method: 'GET',
       contentType: 'text/plain',
           timeout: 60};

urlopen.open(options, function(error, response) {
  if (error)
  {
    console.alert("urlopen error: " + JSON.stringify(error));
    session.output.write("<log />");
  }
  else
  {
    response.readAsBuffer(function(error, responseData){
      if (error)throw error;
      else session.output.write("<log>" + responseData + "</log>") ;
    });
  }
});