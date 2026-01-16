var Qio = require("q-io/http");

Qio.read("http://localhost:1337").then(
  function(data){
    console.log(JSON.parse(data));
  },
  function(err){

  }
).done();