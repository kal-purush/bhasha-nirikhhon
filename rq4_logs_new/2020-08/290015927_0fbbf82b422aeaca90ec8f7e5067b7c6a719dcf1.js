import axios from 'axios';

// This asyncronous function fetches the global data of covid-19
// or the data of a specific country if it's chosen 
export const fetchCovidData = async (country) => {
    try {
        let url = `https://covid19.mathdro.id/api`;

        // If we want the data of a specific country
        // , then the url changes
        if(country) {
            url = `https://covid19.mathdro.id/api/countries/${country}`
        }
        const response = await axios.get(url)

        return {
            confirmed: parseInt(response.data.confirmed.value),
            recovered: parseInt(response.data.recovered.value),
            deaths: parseInt(response.data.deaths.value),
            lastUpdate: response.data.lastUpdate
        }
    } catch (error) {
        return error
    }
}

export const fetchDailyCovidData = async () => {
    try {
        let url = `https://covid19.mathdro.id/api/daily`;
      const { data } = await axios.get(url);
  
      return data.map(({ confirmed, deaths, reportDate: date }) => ({ confirmed: confirmed.total, deaths: deaths.total, date }));
    } catch (error) {
      return error;
    }
  };

export const fetchCountries = async () => {
    try {
        let url = `https://covid19.mathdro.id/api/countries`

        const response = await axios.get(url)

        const data = response.data.countries
        const countries = []

        data.forEach((country) => {
            countries.push(country.name);
        })

        return countries;
    } catch(error) {
        return error
    }
}