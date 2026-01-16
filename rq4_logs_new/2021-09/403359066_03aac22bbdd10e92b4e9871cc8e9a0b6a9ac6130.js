
let currentDate = moment().format('l');

function currentWeather() {

    let searchCity = document.getElementById('locationInput').value;

    fetch(
        'https://api.openweathermap.org/data/2.5/weather?q=' +
        searchCity +
        '&units=imperial' +
        '&appid=668cdb30df14e3d9284e2e3a36347615'
    )

    .then(function(responce){
        return responce.json();
    })

    .then(function(responce){
        

        let todayCity = responce.name;
        let todayIcon = responce.weather.icon;
        let todayTemp = responce.main.temp;
        let todayWind = responce.wind.speed;
        let todayHumidity = responce.main.humidity
        

        // Empty out divs
        $('.city').innerHTML = '';
        $('.temp').innerHTML = '';
        $('.wind').innerHTML = '';
        $('.humidity').innerHTML = '';
        $('.uv').innerHTML = '';

        $('.city').text(todayCity + ' ' + todayIcon + ' ' + currentDate); 
        $('.temp').text('Temp: ' + todayTemp + ' °F');
        $('.humidity').text('Humidity ' + todayHumidity + ' %');
        $('.wind').text('Wind: ' + todayWind + ' MPH');
        $('.uv').text('UV Index:');

    });
}