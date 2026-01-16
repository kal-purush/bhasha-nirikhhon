const APIKEY = 'OpenWeathrMap.org API KEY';

const main=document.getElementById("main");
const form=document.getElementById("myForm");
const loc=document.getElementById("loc");

const url=(searchLoc)=> `https://api.openweathermap.org/data/2.5/weather?q=${searchLoc}&APPID=${APIKEY}`;

async function getWeatherBysearchLoc(searchLoc) {
    const resp=await fetch(url(searchLoc));
    const resp2=await resp.json();
    console.log(resp2);

    addDataToPage(resp2);
}

function addDataToPage(data){
    
    const temp=convertTempToCelsius(data.main.temp);
    const feelTemp=convertTempToCelsius(data.main.feels_like);
    const humidity=data.main.humidity;
    const pressure=data.main.pressure;
    const sunrise= unixtimeToStandard(data.sys.sunrise);
    const sunset=unixtimeToStandard(data.sys.sunset);

    const weather=document.createElement("main");
    weather.classList.add('weather');

    weather.innerHTML=`
    <h3>Data for ${loc.value}, ${data.sys.country}</h3>
    <h2>Temperature: ${temp}&deg; C (${data.weather[0].description}) <img src="http://openweathermap.org/img/wn/${data.weather[0].icon}.png" /> </h2>
    <p>Feels Like: ${feelTemp}&deg; C</p>
    <p>Humidity: ${humidity}%</p>
    <p>Pressure: ${pressure} atm</p>
    <p>Sunrise: ${sunrise} UTC</p>
    <p>Sunset: ${sunset} UTC</p>

    `;

    main.innerHTML="";

    main.appendChild(weather);
}

function convertTempToCelsius(temp) {
    return (temp-273.15).toFixed(2);
}

function unixtimeToStandard(time){
    let unix_timestamp = time;
// Create a new JavaScript Date object based on the timestamp
// multiplied by 1000 so that the argument is in milliseconds, not seconds.
var date = new Date(unix_timestamp * 1000);
// Hours part from the timestamp
var hours = date.getHours();
// Minutes part from the timestamp
var minutes = "0" + date.getMinutes();
// Seconds part from the timestamp
var seconds = "0" + date.getSeconds();

// Will display time in 10:30:23 format
var formattedTime = hours + ':' + minutes.substr(-2) + ':' + seconds.substr(-2);

    return formattedTime;
}

form.addEventListener("submit",(e)=>{
    e.preventDefault();
    const searchLoc=loc.value;
    if(searchLoc){
        getWeatherBysearchLoc(searchLoc);
    }
})

// "https://openweathemap.org/img/w/${data.weather[0].icon}.png"
// src="https://openweathermap.org/themes/openweathermap/assets/vendor/owm/img/widgets/${data.weather[0].icon}.png" width="64" height="64"