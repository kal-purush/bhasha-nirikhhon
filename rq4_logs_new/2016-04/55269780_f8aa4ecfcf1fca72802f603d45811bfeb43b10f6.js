// Create if/then scenario for commands
switch (process.argv[2]) {
  case "my-tweets": 
    displayTweets();
    break;
  case "spotify-this-song":
    // spotify(process.argv[3]);
    break;
  case "movie-this":
    // movie(process.argv[3]);
    break;
  case "do-what-it-says":
    // doRandom();
    break;
  default:
    console.log("Command not valid!");
}