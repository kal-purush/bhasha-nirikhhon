import axios from "axios";
import { AppDispatch } from "../store";
import { IMovieId } from "../../interfaces/IMovie";
import {
  DELETE_MOVIE_ERROR,
  DELETE_MOVIE_STARTED,
  DELETE_MOVIE_SUCCESS,
  DeleteMovieActionType,
} from "../types/deleteMovie";

const API_URL = "http://localhost:4000";

export const deleteMovie = (movieId: number) => (dispatch: AppDispatch) => {
  dispatch(deleteMovieStarted());

  axios
    .delete(`${API_URL}/movies/${movieId}`)
    .then(() => dispatch(deleteMovieSuccess(movieId)))
    .catch((err) => dispatch(deleteMovieError(err)));
};

const deleteMovieStarted = (): DeleteMovieActionType => ({
  type: DELETE_MOVIE_STARTED,
});

const deleteMovieSuccess = (movieId: number): DeleteMovieActionType => ({
  type: DELETE_MOVIE_SUCCESS,
  payload: movieId,
});

const deleteMovieError = (error: Error): DeleteMovieActionType => ({
  type: DELETE_MOVIE_ERROR,
  error: error,
});