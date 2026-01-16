import "babel-polyfill"
import appFactory from "./libs/appFactory";
import browserify = require("browserify")
import config from "./libs/config"


function watchAsyncError(af){
	return async function(req, resp){
		try{
			await af(req, resp)
		}catch(e){
			console.log(e)
			console.log(e.stack)
		}
	}
}

var main = async ()=>{
	let app;
	try{
		app = appFactory.createApp();
	}catch(e){
		console.log(e)
	}


	app.get('/',async function(req, res) {
			res.render('index')
	});

	app.get('/browserify/*', function(req, res) {
		//TODO: cache this in production or generate all files beforehand
		let reqFile:string = req.params[0]
		if(config.env == "DEV"){
			let stream = browserify(["./public/ts/"+reqFile]).bundle()
			stream.on("data", function(buffer){
				res.write(buffer)
			})
			stream.on("end", function(){
				res.end()
			})
		}else{

			res.sendfile(reqFile, {root: './public/compiledTS'});
		}
	});

	app.listen(3000, function(){
	    console.log("Server running");
	});
}
try{
	main()
}catch(err){
	console.log("hit")
}