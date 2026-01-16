
//https://api.openweathermap.org/data/2.5/weather?q=mumbai&units=metric&appid=7e3f21edee540e6110af347b55eb1ab2
const api = {
  key: "7e3f21edee540e6110af347b55eb1ab2",
  units: "metric",
  base: "https://api.openweathermap.org/data/2.5/"
}

const searchbox = document.querySelector('.search-box');
searchbox.addEventListener('keypress', setQuery);

function setQuery(evt) {
  if (evt.keyCode == 13)
    getResult(searchbox.value);
}
function getResult(cityName) {
  console.log(cityName);
  const url = `${api.base}weather?q=${cityName}&units=${api.units}&appid=${api.key}`;
  console.log(url);

  fetch(url).then((response) => {
    console.log(response);
    return response.json();
  })
    .then((responseJson) => {
      console.log(responseJson.cod);
      if (responseJson.cod === 200)
        displayResults(responseJson);
    })
    .catch((err) => {
      console.log("Error in calling API ", err);
    })

}

function displayResults(responseJson) {
  let city = document.querySelector('.city');
  city.innerHTML = `${responseJson.name} ,${responseJson.sys.country}`;

  let temp = document.querySelector('.temp');
  temp.innerHTML = `${Math.round(responseJson.main.temp)} °C`;

  let weather = document.querySelector('.weather');
  weather.innerHTML = `${responseJson.weather[0].main}`;

  let hilow = document.querySelector('.hi-low');
  hilow.innerHTML = `${Math.round(responseJson.main.temp_min)}°c/ ${Math.round(responseJson.main.temp_min)}°c `;

  let now = new Date();
  let date = document.querySelector('.date');
  date.innerHTML = dateBuilder(now);
//https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Intl/DateTimeFormat/DateTimeFormat

  function dateBuilder(date) {
    let months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];
    let days = ['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'];
    let day = days[date.getDay()];
    let month = months[date.getMonth()];
    let year =date.getFullYear();
    return `${day} ${date.getDate()} ${month} ${year}`;
  }

}