var express = require('express');
var bodyParser = require('body-parser');

var app = express();

app.set('port', (process.env.PORT || 3000));
 app.use(express.static('public'));
app.set('views', __dirname + '/public/views');
app.engine('html', require('ejs').renderFile);
app.set('view engine', 'html');

app.use(bodyParser.urlencoded({
  extended: true
}));
app.use(bodyParser.json());


app.connect('3000');

app.get('/', (req, res)=>{
	res.render('fakelook');
});
const port = process.env.PORT || 3000;
app.listen(port,function(){
    console.log("Server has started");
});