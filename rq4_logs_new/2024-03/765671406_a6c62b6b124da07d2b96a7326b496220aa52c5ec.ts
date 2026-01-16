const API_KEY = '6U4WGAH7RT9B5FW3YM8VFD52K';
const BASE_URL = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline';

const weatherService = {
  getCurrentWeather: async (city: string) => {
    const response = await fetch(`${BASE_URL}/${city}?unitGroup=us&key=${API_KEY}&contentType=json`);
    const data = await response.json();
    return {
      locationName: data.resolvedAddress,
      temp: data.currentConditions.temp,
      description: data.currentConditions.description,
      windSpeed: data.currentConditions.windSpeed,
      humidity: data.currentConditions.humidity,
      uvIndex: data.currentConditions.uvindex,
    };
  },

  getForecast: async (city: string) => {
    const response = await fetch(`${BASE_URL}/${city}/daily?unitGroup=us&key=${API_KEY}&contentType=json`);
    const data = await response.json();
    return data.days.map((day: any) => ({
      date: day.datetime,
      temp: day.tempmax,
      description: day.conditions,
      windSpeed: day.windspeed,
      humidity: day.humidity,
    }));
  },
};

export { weatherService };