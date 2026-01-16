const inputBox=document.querySelector('.input-box');
const searchBtn=document.getElementById('searchbtn');
const weather_img=document.querySelector('.weather-img');
const temp=document.querySelector('.temp');
const desc=document.querySelector('.desc');
const humidity=document.getElementById('humidity');
const wind_speed=document.getElementById('wind-speed');

const sunrise=document.getElementById('sunrise');
const sunset=document.getElementById('sunset');

const location_not_found=document.querySelector('.location-not-found');
const weather_body=document.querySelector('.weather_body');

async function checkWeather(city){
    const api_key="1ee3c10d8665bc603d624684dfd37ab8";
    const url=`https://api.openweathermap.org/data/2.5/weather?q=${city}&appid=${api_key}`;

    const weather_data=await fetch(`${url}`).then(response =>response.json());
    if(weather_data.cod === '404'){

        location_not_found.style.display="flex";
        weather_body.style.display="none";
        console.log("error");
        return;
    }

    location_not_found.style.display="none";
    weather_body.style.display="flex";
   
    temp.innerHTML=`${Math.round(weather_data.main.temp-273.15)}°C`;
    desc.innerHTML=`${weather_data.weather[0].description}`;
    humidity.innerHTML=`${weather_data.main.humidity}%`;
    wind_speed.innerHTML=`${weather_data.wind.speed}km/h`;
    
    const sunrise_data=weather_data.sys.sunrise;
    const format_sunrise=new Date(sunrise_data*1000).toLocaleTimeString();
    sunrise.innerHTML=`${format_sunrise}`;

    const sunset_data=weather_data.sys.sunset;
    const format_sunset=new Date(sunset_data*1000).toLocaleTimeString();
    sunset.innerHTML=`${format_sunset}`;

   console.log(weather_data);

// Get the icon code and form the icon URL
const iconCode = weather_data.weather[0].icon;
const iconUrl = `http://openweathermap.org/img/wn/${iconCode}@2x.png`;
weather_img.src = iconUrl;   
}

searchBtn.addEventListener('click',()=>{
    checkWeather(inputBox.value)
});
inputBox.addEventListener('keypress',(Event)=>{
    if(Event.key==='Enter'){
        checkWeather(inputBox.value);
    }
});

