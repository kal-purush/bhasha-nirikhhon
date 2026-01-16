// index2.js

const { fetchMyIP } = require('./iss_promised');

  fetchMyIP()
  .then(fetchCoordsByIP)
  .then(fetchISSFlyOverTimes)
  .then(body => console.log(body));


const { nextISSTimesForMyLocation } = require('./iss_promised');

// see index.js for printPassTimes 
// copy it from there, or better yet, moduralize and require it in both files

nextISSTimesForMyLocation()
  .then((passTimes) => {
    printPassTimes(passTimes);
  })