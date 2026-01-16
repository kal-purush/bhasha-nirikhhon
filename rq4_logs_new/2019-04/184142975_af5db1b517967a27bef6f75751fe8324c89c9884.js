let axios = require("axios");
let fs = require("fs");

require("dotenv").config();

let bands = process.env.BANDS_ID;
let spotify = process.env.SPOTIFY_ID;
let omdb = process.env.OMDB_ID;

require("./keys.js");

let moment = require('moment');

let UserInput = function() {
    
    let divider = "\n------------------------------------------------------------\n\n";

    this.findConcert = function (term) {
        let URL = "https://rest.bandsintown.com/artists/" + term + "/events?app_id=" + bands;

        axios.get(URL).then(function(response) {

        let jsonData = response.data
        // console.log(jsonData);
        for (let i = 0; i < jsonData.length; i++){

        let concertData = [
            "Name of Venue: " + jsonData[i].venue.name,
            "Venue Location: " + jsonData[i].venue.city,
            "Date of Event: " + jsonData[i].datetime,
            ].join("\n");

            // moment(concertData[2]).format(MM/DD/YYYY);

        fs.appendFile("log.txt", concertData + divider, function(err) {
          if (err) throw err;
          console.log(concertData + divider);
        });
        };
    });
  };

    this.findSong = function (term) {
        let URL = "https://api.spotify.com/v1" + term + "?app_id=" + spotify; 
        
        axios.get(URL).then(function(response) {    
        
        let jsonData = response.data
        console.log(jsonData);
        
        // fs.appendFile("log.txt", jasonData + divider, function(err) {
        //   if (err) throw err;
        //   console.log(showData);
        // });
    });
  };

    this.findMovie = function (term) {

      let OMDB = process.env.OMDB_ID;

        let URL = "http://www.omdbapi.com/?apikey=" + omdb + "&t=" + term;

        axios.get(URL).then(function(response) {

        let jsonData = response.data;
            // console.log(jsonData);
        let movieData = [
        "Title: " + jsonData.Title,
        "Year Released: " + jsonData.Year,
        "IMDB Rating: " + jsonData.Ratings[0].Value,
        "Rotten Tomatoes Rating: " + jsonData.Ratings[1].Value,
        "Country Produced in: " + jsonData.Country,
        "Language: " + jsonData.Language,
        "Plot: " + jsonData.Plot,
        "Actors: " + jsonData.Actors
        ].join("\n");

        fs.appendFile("log.txt", movieData + divider, function(err) {
          if (err) throw err;
          console.log(movieData + divider);
        });
    });
  };

    // this.findDoit = function (do) {
        // let URL = "https://rest.bandsintown.com/artists/" + term + "/events?app_id=codingbootcamp";
// 
        // axios.get(URL).then(function(response) {
// 
        // let jsonData = response.data
        // console.log(jasonData);
// 
        // fs.appendFile("log.txt", jasonData + divider, function(err) {
        //   if (err) throw err;
        //   console.log(showData);
        // });
    // });
//   };
}

module.exports = UserInput;