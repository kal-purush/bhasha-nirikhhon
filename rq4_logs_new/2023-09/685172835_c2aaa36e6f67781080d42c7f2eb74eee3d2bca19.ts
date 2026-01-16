// get the long and lat from the addresses

const getLongLat = (address : string) => {
    var fetch = require('node-fetch');
    var requestOptions = {
    method: 'GET',
    };

    fetch("https://api.geoapify.com/v1/geocode/search?text=38%20Upper%20Montagu%20Street%2C%20Westminster%20W1H%201LJ%2C%20United%20Kingdom&apiKey=YOUR_API_KEY", requestOptions)
    //@ts-ignore
    .then(response => response.json())
    //@ts-ignore
    .then(result => console.log(result))
    //@ts-ignore
    .catch(error => console.log('error', error));
};