
var request = require('request');


module.exports = {
  getUpcomingMovieData: function() {
    return new Promise(function(fulfill, reject){
      request('https://api.themoviedb.org/3/movie/upcoming/?sort_by=popularity.desc&api_key=b2c319d64cba3280f7ee6977b9a470e0',
        function(error, response, data){
          console.log('data ', data);
          // console.log('response ', response);
          if (error) reject(error);
          else fulfill(data);
        });
    });
  }
}

// function getUpcomingMovieData() {
//   return new Promise(function(fulfill, reject){
//     request('https://api.themoviedb.org/3/movie/upcoming/?sort_by=popularity.desc&api_key=b2c319d64cba3280f7ee6977b9a470e0',
//       function(error, response, data){
//         // console.log('data ', data);
//         // console.log('response ', response);
//         if (error) reject(error);
//         else fulfill(data);
//       });
//   });
// }