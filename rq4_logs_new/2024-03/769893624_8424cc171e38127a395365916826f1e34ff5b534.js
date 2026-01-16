const currentDate = new Date();
const options = { weekday: "long", hour: "2-digit", minute: "2-digit" };
const formattedDate = new Intl.DateTimeFormat("en-US", options).format(
  currentDate
);

document.getElementById("currentDateTime").textContent = formattedDate;

document
  .getElementById("searchForm")
  .addEventListener("submit", function (event) {
    event.preventDefault();
    const cityInput = document.getElementById("cityInput");
    const city = cityInput.value;
    document.getElementById("cityName").textContent = city;
    getWeather(city); // Вызываем функцию для получения погоды
  });

function getWeather(city) {
  let apiKey = "59a99bf814c1d687d082fbb625caab0c";
  let url = `https://api.openweathermap.org/data/2.5/weather?q=${city}&appid=${apiKey}&units=metric`;

  axios
    .get(url)
    .then((response) => {
      console.log(response.data);
      let temperature = response.data.main.temp;
      updateTemperature(temperature);
    })
    .catch((error) => {
      console.error("Error fetching weather data:", error);
    });
}

function updateTemperature(newTemperature) {
  document.querySelector(".current-temperature-water").textContent =
    Math.round(newTemperature); // Округляем температуру до ближайшего целого
}