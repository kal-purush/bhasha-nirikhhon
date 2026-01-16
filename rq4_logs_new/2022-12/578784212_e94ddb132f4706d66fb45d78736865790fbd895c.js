let weatherWidget = document.querySelector(".weather-widget");

let cityName = prompt("Название города ?");
if (cityName === "" || cityName === null) {
  weatherWidget.classList.add("hide");
  alert("Некорректный ввод");
} else {
  fetch(
    `http://api.openweathermap.org/data/2.5/weather?q=${cityName}&units=metric&APPID=5d066958a60d315387d9492393935c19`
  )
    .then((res) => res.json())
    .then((data) => {
      let city = document.querySelector(".name");
      let temp = document.querySelector(".temp");
      let pressure = document.querySelector(".pressure");
      let description = document.querySelector(".description");
      let humidity = document.querySelector(".humidity");
      let speed = document.querySelector(".speed");
      let deg = document.querySelector(".deg");
      let icon = document.querySelector(".icon");

      city.innerHTML = data.name;
      temp.innerHTML = data.main.temp + "°C";
      pressure.innerHTML = data.main.pressure + " hPa";
      description.innerHTML = "Description: " + data.weather[0].description;
      humidity.innerHTML = data.main.humidity + "φ";
      speed.innerHTML = data.wind.speed + " m/sec";
      deg.innerHTML = data.wind.deg + "°";
      icon.src =
        "http://openweathermap.org/img/w/" + data.weather[0].icon + ".png";
    });
}