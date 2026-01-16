




var action = process.argv[2];
var value = process.argv[3];


var request = require('request');

var spotify = require("spotify");




		//switch is a function that called directly
		// action is a global variable takes place at process.argv[2] which is a user input
switch (action) {
		
	case 'my-tweets':
		tweets();
		break;
		//if USER TYPES: "spotify" perform the function 'spotify-this-song'
	case 'spotify-this-song':
		spotify();
		break;
		//if USER TYPES: "omdb" perform the function 'movie-this'
	case 'movie-this':
		omdb();
		break;
		//if USER TYPES: "random" perform functions 'do-what-it-says'
	case 'do-what-it-says':
		random();
		break;
}

// my-tweets function begins//
function tweets() {
	var fs = require('fs');

	var keys = require('./keys.js');

	var Twitter = require('twitter');

	var client = new Twitter(keys.twitterKeys);


	var params = {screen_name: 'erespinoza', count: 20};
	
	client.get('statuses/user_timeline', params, function(error, tweets, response) {
  	
  	if (!error) {
  		var data = []; //empty array to hold data
  		for(var i = 0; i < tweets.length; i++) {
  			data.push(
  				'created at: ' : tweets[i].created_at,
  				'Tweets: ' : tweets[i].text,
  			});
  		} 
  		console.log(data) //which data array was created in row 47

  		}
	});
};

  	

function spotify() {
	var spotify = new Spotify({
  		id: "9573aa92802f45aa86130300ade99be2",
  		secret: "37cd636287a741b18f3d8bd748c6375f"
	});
 
	spotify.search({ type: 'track', query: 'All the Small Things' }, function(err, data) {
  		if (err) {
    	return console.log('Error occurred: ' + err);
  	}
 
	console.log(data); 
	});
	};

function omdb() {
	var nodeArgs = process.argv;
	var movieName = "";

	for (var i = 2; i < nodeArgs.length; i++)
		
		
			if (i > 2 && i < nodeArgs.length) {
				movieName = movieName + "+" + nodeArgs[i];
			}
			else {
				movieName += nodeArgs[i];
			}
	}
	var queryURL = "http://omdbapi.com/?t=" + movieName + "&y=plot=short&apikey=40e9cece";
	request(queryURL,function(error, response,body) {
		if (!error && response.statusCode === 200) {
			console.log("Title:" + JSON.parse(body).Title);
			console.log("Year:" + JSON.parse(body).Year);
			console.log("IMDB Rating:" + JSON.parse(body).imdbRating);
			console.log("Country:" + JSON.parse(body).Country);
			console.log("Language:" + JSON.parse(body).Language);
			console.log("Plot:" + JSON.parse(body).Plot);
			console.log("Actors:" + JSON.parse(body).Actors);
			console.log("Rotten Tomatoes Rating" + JSON.parse(body).tomatoRating);
			console.log("Rotten Tomatoes URL:" + JSON.parse(body).tomatoURL);
			console.log(' ');
			fs.appendfile(log.txt, ('============== LOG ENTRY BEGIN =============\r\n' + Date() + '\r\n \r\nTERMINALCOMMAND'))
				if (error) throw err;


		}
	});

function random() {
			// boilerplate fs.readfile function given
			//reads the file in the random.txt, if no data log error
	fs.readfile('random.txt', 'utf8', function(error,data) {
		if(error) {
			//if error shown write error to console like: "error: error descrip"
			console.log('error:', error);
		} else {
			//or else if no errors write data read from the random.txt file
			//but first create a new array called DataArr
			//then split the data with commas like: "data,(space)data"
			var dataArr = data.split(', ');
			//if the data TYPED BY THE USER equals 'spotify' which user inputted at index 0
			//then execute the function 'spotify'
			//using the data in the dataArray located at index [1]
			if(dataArr[0] === 'spotify') {
				spotify-this-song(dataArr[1]);
			} 
			//line101: if the data TYPED BY THE USER equals 'omdb' which user inputted at index 0
			//line102: then execute the function 'omdb'
			//line102: using the data in the dataArray located at index [1]
			if(dataArr[0] === 'omdb') {
				movie-this(dataArr[1]);
			}
		}
	});
}
