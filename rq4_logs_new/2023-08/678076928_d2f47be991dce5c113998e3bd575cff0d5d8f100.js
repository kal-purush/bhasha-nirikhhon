let apiKey = "3d3bc046438e1c78a004d487138fe919";
let units = "metric";
let isFahrenheit = false;

const city = document.querySelector("#city");
const searchButton = document.querySelector("#search");
const currentLocationButton = document.querySelector(".btn-success");
const state = document.querySelector("#state");
const temperature = document.querySelector("#temperature");
const timeInfo = document.querySelector("#time");
const description = document.querySelector("#description");
const humidity = document.querySelector("#humidity");
const wind = document.querySelector("#wind");

function showWeather(response) {
  state.textContent = response.data.name;
  description.textContent = response.data.weather[0].description;
  temperature.textContent = Math.round(response.data.main.temp);
  humidity.textContent = response.data.main.humidity;
  wind.textContent = Math.ceil(response.data.wind.speed);
}

function getWeatherByCity(cityName) {
  let apiUrl = `https://api.openweathermap.org/data/2.5/weather?q=${cityName}&appid=${apiKey}&units=${units}`;
  axios.get(apiUrl).then(showWeather);
}

searchButton.addEventListener("click", () => {
  if (city.value.trim() !== "") {
    getWeatherByCity(city.value);
  }
  city.value = "";
});

function getCurrentLocationWeather(latitude, longitude) {
  let apiUrl = `https://api.openweathermap.org/data/2.5/weather?lat=${latitude}&lon=${longitude}&appid=${apiKey}&units=${units}`;
  axios.get(apiUrl).then(showWeather);
}

currentLocationButton.addEventListener("click", () => {
  if (navigator.geolocation) {
    navigator.geolocation.getCurrentPosition(function (position) {
      const latitude = position.coords.latitude;
      const longitude = position.coords.longitude;
      getCurrentLocationWeather(latitude, longitude);
    });
  }
});

function updateDateTime() {
  const now = new Date();
  const options = { weekday: "long", hour: "numeric", minute: "numeric" };
  const formattedDateTime = now.toLocaleDateString("en-US", options);
  timeInfo.textContent = formattedDateTime;
}

updateDateTime();

function toFahrenheit() {
  if (isFahrenheit === false) {
    const fahrenheitValue = Math.round((temperature.textContent * 9) / 5 + 32);
    temperature.textContent = fahrenheitValue;
  }
  isFahrenheit = true;
}

function toCelsius() {
  if (isFahrenheit === true) {
    const celsiusValue = Math.round(((temperature.textContent - 32) * 5) / 9);
    temperature.textContent = celsiusValue;
  }
  isFahrenheit = false;
}

const celsius = document.querySelector("#celsius");
const fahrenheit = document.querySelector("#fahrenheit");

fahrenheit.addEventListener("click", toFahrenheit);
celsius.addEventListener("click", toCelsius);