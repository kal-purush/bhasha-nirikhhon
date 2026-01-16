import axios from "axios";
import { AppDispatch } from "../store";
import { IMovie } from "../../interfaces/IMovie";
import {
  ADD_MOVIE_ERROR,
  ADD_MOVIE_STARTED,
  ADD_MOVIE_SUCCESS,
  AddMovieActionType,
  ADD_MOVIE_FINISHED,
} from "../types/addMovie";

const API_URL = "http://localhost:4000";

export const addMovie = (movieData: IMovie) => (dispatch: AppDispatch) => {
  dispatch(addMovieStarted());

  axios
    .post(`${API_URL}/movies`, movieData)
    .then((response) => dispatch(addMovieSuccess(response.data)))
    .catch((err) => dispatch(addMovieError(err)));
};

export const addMovieFinished = (): AddMovieActionType => ({
  type: ADD_MOVIE_FINISHED,
});

const addMovieStarted = (): AddMovieActionType => ({
  type: ADD_MOVIE_STARTED,
});

const addMovieSuccess = (movies: IMovie): AddMovieActionType => ({
  type: ADD_MOVIE_SUCCESS,
  payload: movies,
});

const addMovieError = (error: Error): AddMovieActionType => ({
  type: ADD_MOVIE_ERROR,
  error: error,
});