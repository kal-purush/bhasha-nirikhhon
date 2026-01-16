class Weather {
  async getWeather(city: string) {
    const API_KEY = "";
    try {
      const response = await fetch(
        `https://api.openweathermap.org/data/2.5/weather?q=${city}&appid=${API_KEY}`
      );

      if (!response.ok) {
        throw new Error(`Failed to fetch weather data: ${response.statusText}`);
      }

      const data = await response.json();
      console.log(data);
      return data;
    } catch (error) {
      console.error("Error fetching weather data:", error);
      throw error;
    }
  }
}

const weatherInstance = new Weather();

export default weatherInstance;