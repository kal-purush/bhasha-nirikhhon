import axios from "axios";
import { AppDispatch } from "../store";
import { IMovie } from "../../interfaces/IMovie";
import {
  UPDATE_MOVIE_ERROR,
  UPDATE_MOVIE_STARTED,
  UPDATE_MOVIE_SUCCESS,
  UpdateMovieActionType,
  UPDATE_MOVIE_FINISHED,
} from "../types/updateMovie";

const API_URL = "http://localhost:4000";

export const updateMovie = (movieData: IMovie) => (dispatch: AppDispatch) => {
  dispatch(updateMovieStarted());

  axios
    .put(`${API_URL}/movies`, movieData)
    .then((response) => dispatch(updateMovieSuccess(response.data)))
    .catch((err) => dispatch(updateMovieError(err)));
};

export const updateMovieFinished = (): UpdateMovieActionType => ({
  type: UPDATE_MOVIE_FINISHED,
});

const updateMovieStarted = (): UpdateMovieActionType => ({
  type: UPDATE_MOVIE_STARTED,
});

const updateMovieSuccess = (movie: IMovie): UpdateMovieActionType => ({
  type: UPDATE_MOVIE_SUCCESS,
  payload: movie,
});

const updateMovieError = (error: Error): UpdateMovieActionType => ({
  type: UPDATE_MOVIE_ERROR,
  error: error,
});