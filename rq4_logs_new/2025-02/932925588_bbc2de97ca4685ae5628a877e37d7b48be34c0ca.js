

const apiKey = "b37753d42bf17cf2e0f832a920ee0154";
const apiURL = "https://api.openweathermap.org/data/2.5/weather?units=metric&q=";

async function checkWeather(city) {
    const response = await fetch(apiURL + city + `&appid=${apiKey}`);
    const data = await response.json();

    if(response.status == 404){
        document.querySelector(".error").style.display = "block";
        document.querySelector(".weather").style.display = "none";
    }
    else{
        const icon = data.weather[0].main;
        const weatherIcon = document.querySelector(".weather img")
        if(icon=="Haze"){
            weatherIcon.src = "./images/weather_18437584.png";
        }
        else if(icon == "Clear"){
            weatherIcon.src = "./images/brightness_16783013.png";
        }
        else if(icon == "Snow"){
            weatherIcon.src = "./images/snowflake_17681911.png";
        }
        else if(icon == "Clouds"){
            weatherIcon.src = "./images/rain_17032999.png";
        }
        else{
            weatherIcon.src = "./images/sun_17142295.png";
        }

        document.querySelector(".city").innerHTML = data.name;
        document.querySelector(".temp").innerHTML = Math.round(data.main.temp) + "°C";
        document.querySelector(".humidity").innerHTML = data.main.humidity+"%";
        document.querySelector(".wind").innerHTML = data.wind.speed+" Km/h";
    
        document.querySelector(".weather").style.display="block";
        document.querySelector(".error").style.display = "none";
    }
}

const city_name = document.querySelector(".search input");
const searchBtn = document.querySelector(".search button");

searchBtn.addEventListener("click",()=>{
    checkWeather(city_name.value);
})



city_name.addEventListener("keydown",(event)=>{
    console.log(event);
    if(event.key === "Enter"){
        checkWeather(city_name.value);
    }
})