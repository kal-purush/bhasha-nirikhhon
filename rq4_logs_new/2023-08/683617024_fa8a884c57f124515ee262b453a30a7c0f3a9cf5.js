const searchbox = document.querySelector("#cityinput");
const searchbtn = document.querySelector("#add");
const apikey = "17bf8e4ee7ffe5bc94b5ed59c00e85ba";
const apiurl = "https://api.openweathermap.org/data/2.5/weather?units=metric&q=";
async function cheakWheather(city) {
    const response = await fetch(apiurl + city + `&appid=${apikey}`);
    var data = await response.json();
    console.log(data);
    document.querySelector("#cityoutput").innerHTML = "City:-" + data.name;
    document.querySelector("#temp").innerHTML = "Temprature:-" + data.main.temp + " ℃";
    document.querySelector("#condi").innerHTML = "Cloud:-" + data.weather[0].description;
    document.querySelector("#humidity").innerHTML = "Humidity:-" + data.main.humidity + " %";
    document.querySelector("#pressure").innerHTML = "Pressure:-" + data.main.pressure + " hpa";
    document.querySelector("#temp1").innerHTML = "Temprature:-" + data.main.temp + " ℃";
    document.querySelector("#feels_like").innerHTML = "Fells like:-" + data.main.temp + " ℃";
    document.querySelector("#temp_max").innerHTML = "Max temp:-" + data.main.temp + " ℃";
    document.querySelector("#temp_min").innerHTML = "Min temp:-" + data.main.temp + " ℃";
    document.querySelector("#description").innerHTML = "Condition:-" + data.weather[0].description;
    const sunrise = new Date(data.sys.sunrise * 1000);
    document.querySelector("#sunrise").innerHTML = "Sunrise:-" + sunrise;
    const sunset = new Date(data.sys.sunset * 1000);
    document.querySelector("#sunset").innerHTML = "Sunset:-" + sunset;
    document.querySelector("#speed").innerHTML = "Wind speed:-" + data.wind.speed;
    document.querySelector("#deg").innerHTML = "Deg:-" + data.wind.deg + " °";
    document.querySelector("#gust").innerHTML = "Gust:-" + data.wind.gust;

}
searchbtn.addEventListener("click", () => {
    cheakWheather(searchbox.value);
});