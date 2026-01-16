import express = require("express")
import bodyParser = require("body-parser");

export default {
	createApp: function(){
		var app = express()
		app.set('views', __dirname + '/../views');
		app.set('view engine', 'jade');
		app.set('view options', { layout: false });
		
		app.use(bodyParser.urlencoded({ extended: true }));
		app.use(bodyParser.json());
		
		app.use("/public", express.static(__dirname + '/../public'));
		app.use("/bower_components", express.static(__dirname + '/../bower_components'));
		
		return app
	}
}