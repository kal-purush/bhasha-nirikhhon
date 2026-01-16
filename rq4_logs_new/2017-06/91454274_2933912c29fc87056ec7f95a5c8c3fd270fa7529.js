import { reverseIPAyekweather } from '../music.json';

async function getWeather(latitude, longitude) {
  const url = `http://api.openweathermap.org/data/2.5/weather?lat=${latitude
  }&lon=${longitude}&APPID=${reverseIPAyekweather}`;
  const response = await fetch(url);

  const jsonResponse = await response.json();
  return jsonResponse;
}

export default getWeather;