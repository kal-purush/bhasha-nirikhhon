import axios from 'axios';
import { TMDBMovie, MoviesResponse } from 'types';

const TMDB_GET_MOVIES_LIST = `https://api.themoviedb.org/3/trending/movie/week?api_key=${process.env.REACT_APP_TMBD_KEY}`;
const YTS_GET_MOVIES_LIST = (page = 1) => `https://yts.mx/api/v2/list_movies.json?page=${page}`;

const getRequestList = (url: string) => {
	const source = axios.CancelToken.source();
	return axios.get(url, {
		cancelToken: source.token,
	});
};

export const TMDB_GET_MOVIE_POSTER = (image: string) => `https://image.tmdb.org/t/p/original${image}`;

export const getMoviesList = async (): Promise<MoviesResponse> => {
	const url = YTS_GET_MOVIES_LIST();
	const {
		data: { data },
	} = await getRequestList(url);

	return data;
};

export const getTopWeekMovies = async (): Promise<TMDBMovie[]> => {
	const {
		data: { results },
	} = await getRequestList(TMDB_GET_MOVIES_LIST);

	return results;
};