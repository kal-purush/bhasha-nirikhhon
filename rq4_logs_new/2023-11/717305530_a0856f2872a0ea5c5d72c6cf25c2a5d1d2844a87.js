const apiKey = "77c6b5343d0ded3c0152fea2a2d36209";
const apiUrl = "https://api.openweathermap.org/data/2.5/weather?units=metric&q=";


const searchBox = document.querySelector(".search input");
const searchBtn = document.querySelector(".search button");
const WeatherIcon = document.querySelector(".weather-icon");


 
async function checkWeather(city){
const response = await fetch(apiUrl + city + `&appid=${apiKey}`);

if(response.status == 404){
       document.querySelector(".error").style.display = "block";
       document.querySelector(".weather").style.display = "none";
}else{
var data = await response.json();

document.querySelector(".city").innerHTML = data.name;
document.querySelector(".temp").innerHTML =  Math.round(data.main.temp) + " °C";
document.querySelector(".humidity").innerHTML = data.main.humidity+ " %";
document.querySelector(".wind").innerHTML = data.wind.speed+ " km/H";
   

  //  THIS SECTION IS USED FOR CHANGING THE CLOUD ICONS----------
if(data.weather[0].main == "clouds"){
  WeatherIcon.src = "images/clouds.png";
}
else if(data.weather[0].main == "clear"){
  WeatherIcon.src = "images/clear.png";
}
else if(data.weather[0].main == "Rain"){
  WeatherIcon.src = "images/rain.png";
}
else if(data.weather[0].main == "Drizzle"){
 WeatherIcon.src = "images/drizzle.png";
}
else if(data.weather[0].main == "Mist"){
  WeatherIcon.src = "images/mist.png";
}

// -----------THIS SECTION IS USED FOR SMALLING THE DISPLAY-----------  

document.querySelector(".weather").style.display = "block";
//    document.querySelector(".error").style.display = "block";

}

}


searchBtn.addEventListener("click" , ()=>{
checkWeather(searchBox.value);
})