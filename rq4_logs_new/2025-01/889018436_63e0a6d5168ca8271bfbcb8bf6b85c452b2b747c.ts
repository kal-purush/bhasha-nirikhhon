import { movieApi } from '../api/instance';
import {
  IMovieCreditResponse,
  IMovieDetailResponse,
  IMovieVideoResponse,
  IPopularMovieResponse,
  ISimilarMovieResponse,
} from './movie.types';
import { convertSnakeToCamel } from '@/utils/formatters';

export const getPopularMovies = async (params = {}): Promise<IPopularMovieResponse> => {
  const { data } = await movieApi.get('/movie/popular', {
    params: {
      language: 'ko-KR',
      ...params,
    },
  });
  return convertSnakeToCamel(data);
};

export const getMovieDatil = async (id: number, params = {}): Promise<IMovieDetailResponse> => {
  const { data } = await movieApi.get(`/movie/${id}`, {
    params: {
      language: 'ko-KR',
      ...params,
    },
  });
  return convertSnakeToCamel(data);
};

export const getMovieVideo = async (id: number, params = {}): Promise<IMovieVideoResponse> => {
  const { data } = await movieApi.get(`/movie/${id}/videos`, {
    params: {
      ...params,
    },
  });
  return convertSnakeToCamel(data);
};

export const getMovieCredit = async (id: number, params = {}): Promise<IMovieCreditResponse> => {
  const { data } = await movieApi.get(`/movie/${id}/credits`, {
    params: {
      language: 'ko-KR',
      ...params,
    },
  });
  return convertSnakeToCamel(data);
};

export const getSimilarMovies = async (id: number, params = {}): Promise<ISimilarMovieResponse> => {
  const { data } = await movieApi.get(`/movie/${id}/similar`, {
    params: {
      language: 'ko-KR',
      ...params,
    },
  });
  return convertSnakeToCamel(data);
};