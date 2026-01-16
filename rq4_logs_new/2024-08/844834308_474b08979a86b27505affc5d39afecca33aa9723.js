const apiKey = "4f74ebf2feec6632d4058ce309e0e10a";
const apiUrl = "https://api.openweathermap.org/data/2.5/weather?units=metric&q=";
const searchBox = document.querySelector(".search input");
const searchBtn = document.querySelector(".search button");
const weatherIcon = document.querySelector(".weather-icon");

async function checkWeather(city){
    const response = await fetch(apiUrl + city + `&appid=${apiKey}`);

    if(response.status == 404){
        document.querySelector(".error").style.display = "block";
        document.querySelector(".weather").style.display = "none";
    }else{
        var data = await response.json();


        document.querySelector(".city").innerHTML = data.name;
        document.querySelector(".temp").innerHTML = Math.round(data.main.temp) + "°C";
        document.querySelector(".humidity").innerHTML = data.main.humidity + "%";
        document.querySelector(".wind").innerHTML = data.wind.speed + " km/h";

        if(data.weather[0].main == "Clouds"){
            weatherIcon.src = "https://i.postimg.cc/W1KyTM5M/clouds.png";
        }
        else if(data.weather[0].main == "Clear"){
            weatherIcon.src = "https://i.postimg.cc/6QRxS9dL/clear.png";
        }
        else if(data.weather[0] == "Rain"){
            weatherIcon.src = "https://i.postimg.cc/ZnFndXXc/rain.png";
        }
        else if(data.weather[0].main == "Drizzle"){
            weatherIcon.src = "https://i.postimg.cc/zv5mgzLP/drizzle.png";
        }
        else if(data.weather[0] == "Mist"){
            weatherIcon.src = "https://i.postimg.cc/Qx2QkFNt/mist.png";
        }

        document.querySelector(".weather").style.display = "block";
        document.querySelector(".error").style.display = "none";

        }
            
    }

    searchBtn.addEventListener("click", () => {
        checkWeather(searchBox.value);
    })
