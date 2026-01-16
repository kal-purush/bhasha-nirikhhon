import axios from "axios";

export async function handler(event) {
  try {
    const { input } = JSON.parse(event.body);

    const response = await axios.get(`https://geo.ipify.org/api/v2/country,city?apiKey=${process.env.VITE_API_KEY}&ipAddress=${input}`);
    const data = response.data;

    return {
      statusCode: 200,
      body: JSON.stringify({ data })
    };
  } catch (err) {
    console.error(err);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: 'API response failed' })
    };
  }
};