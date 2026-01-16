var express =  require("express");
var converter = require('./timeConverter.js')
var app = express();

var port = process.env.PORT || 5000;
var router = express.Router();              // get an instance of the express Router

// test route
app.use(router);

app.get('/:URI',(req,res)=>{
  var query = req.params.URI;
  var resObject = {
    "unix": 0,
    "natural": ''
  }
  if(/^\d+$/.test(query)){ // the uri passed in is unix
    resObject["natural"]=converter.convertToTime(query);
    if(resObject["natural"]==null){
      resObject["unix"]=null;
    }else
    resObject["unix"] = query;
  }
  else{
    resObject["unix"]=converter.convertToUnix(query);
    if(resObject["unix"]==null){
      resObject["natural"]=null;
    }
    else
    resObject["natural"] = query;
  }

  res.json(resObject);
  //res.json({message :  'success'});
});
app .listen(port, ()=>{
console.log('Our app is running on' + port);
});