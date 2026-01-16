import axios from "axios";

const getMovieList = async (title) => {
  const options = {
    method: "GET",
    url: `https://moviesdatabase.p.rapidapi.com/titles/search/title/${title}`,
    params: {
      exact: "true",
      titleType: "movie",
      limit: "5",
    },
    headers: {
      "X-RapidAPI-Key": "b1a45e8a00mshbd6942a92cb4229p1cbeecjsn4081074d23ae",
      "X-RapidAPI-Host": "moviesdatabase.p.rapidapi.com",
    },
  };

  try {
    const response = await axios.request(options);
    return response.data.results;
  } catch (error) {
    console.error(error);
  }
};

export default getMovieList;