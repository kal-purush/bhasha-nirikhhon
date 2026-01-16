import axios from "axios";
import { IMovie } from "../interfaces/IMovie";

const API_URL = "http://localhost:4000";

interface IRequestParams {
  sortBy: string;
  sortOrder: string;
  filter: string;
  limit: number;
}

const addMovie = (movieData: IMovie) =>
  axios.post(`${API_URL}/movies`, movieData);

const deleteMovie = (movieId: number) =>
  axios.delete(`${API_URL}/movies/${movieId}`);

const updateMovie = (movieData: IMovie) =>
  axios.put(`${API_URL}/movies`, movieData);

const loadMovies = (params: IRequestParams) =>
  axios.get(`${API_URL}/movies`, { params });

export const api = {
  addMovie,
  deleteMovie,
  updateMovie,
  loadMovies,
};