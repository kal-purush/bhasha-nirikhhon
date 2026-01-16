import axios from "axios";

const API_URL='https://prueba-tecnica-production-f225.up.railway.app';

export const getMovies = async () => {
    try {
      const url_movies = `${API_URL}/results`;
  
      const { data } = await axios.get(url_movies);
  
      return data;
    } catch (error) {
      return {
        error,
        data: null,
      };
    }
  }
