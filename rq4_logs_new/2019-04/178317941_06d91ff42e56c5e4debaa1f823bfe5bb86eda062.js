const axios = require('axios');

function apiStarWars(){
   return  axios.get('https://swapi.co/api/planets/3')
}

const dados = apiStarWars(); 

module.exports = dados;



