
var path    = require("path");

var express =  require("express"),
    app     =  express();

app.use(express.static(path.join(__dirname, './static')));


app.listen(3000, function(){
  console.log("listening on port 3000");
})