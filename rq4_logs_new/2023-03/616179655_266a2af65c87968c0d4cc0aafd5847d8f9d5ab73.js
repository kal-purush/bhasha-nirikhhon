import { SERVER, FORM, TAB_NOW } from "./scripts/constants.js";
import {
  ADDED_LOCATIONS_LIST,
  createLocalStorage,
} from "./scripts/localStorage.js";
import { clearAddedLocations, clearTabs } from "./scripts/clearFunctions.js";
import { fillTabNow, fillTabDetails } from "./scripts/fillTabs.js";

FORM.FORM_FIELD.addEventListener("submit", getCityWeather);
TAB_NOW.BUTTON_ADD_CITY.addEventListener("click", addFavoiriteCity);

async function getDataFromURL(cityName) {
  const URL = `${SERVER.SERVER_URL}?q=${cityName}&appid=${SERVER.API_KEY}&units=metric`;
  try {
    const response = await fetch(URL);
    if (response.ok) {
      return await response.json();
    } else {
      alert(`HTTP Error: ${response.status} - ${response.statusText}`);
      return;
    }
  } catch (error) {
    alert(new Error(`Error: ${error.name} - ${error.message}`));
  }
}

async function getCityWeather(event) {
  event.preventDefault();
  const cityName = FORM.CITY_NAME.value;
  FORM.FORM_FIELD.reset();
  const cityWeatherData = await getDataFromURL(cityName);

  if (!cityWeatherData) {
    return;
  }

  fillTabNow(cityName, cityWeatherData);
  fillTabDetails(cityName, cityWeatherData);
}

function addFavoiriteCity() {
  const cityName = TAB_NOW.CURRENT_CITY_NAME.textContent;
  if (ADDED_LOCATIONS_LIST.find((city) => city.name === cityName)) {
    alert("This city already exists in list");
    return;
  }
  ADDED_LOCATIONS_LIST.push({ name: cityName });
  createLocalStorage(ADDED_LOCATIONS_LIST);
  render();
}

async function getFavoiriteCityWeather() {
  const cityName = this.parentElement.firstChild.textContent;
  const cityWeatherData = await getDataFromURL(cityName);
  fillTabNow(cityName, cityWeatherData);
  fillTabDetails(cityName, cityWeatherData);
}

function render() {
  clearAddedLocations();
  ADDED_LOCATIONS_LIST.forEach((city) => {
    const addedLocationsList = document.querySelector(".addedLocationsList");
    const favoiriteCity = document.createElement("li");
    const favoiriteCityName = document.createElement("div");
    const closeButton = document.createElement("button");
    favoiriteCity.classList.add("favoirite-city");
    closeButton.classList.add("button", "closeButton");
    closeButton.innerHTML = "X";
    addedLocationsList.appendChild(favoiriteCity);
    favoiriteCity.append(favoiriteCityName, closeButton);
    favoiriteCityName.textContent = city.name;

    closeButton.addEventListener("click", deleteTask);
    favoiriteCityName.addEventListener("click", getFavoiriteCityWeather);
  });
}

function deleteTask() {
  const cityNameForDelete = this.parentElement.firstChild.textContent;
  const cityIndex = ADDED_LOCATIONS_LIST.findIndex(
    (city) => city.name === cityNameForDelete
  );
  if (cityIndex != -1) {
    ADDED_LOCATIONS_LIST.splice(cityIndex, 1);
  }
  createLocalStorage(ADDED_LOCATIONS_LIST);
  clearTabs();
  render();
}

render();