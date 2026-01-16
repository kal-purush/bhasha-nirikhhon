import { createSlice, createAsyncThunk } from '@reduxjs/toolkit'
import { SingleMovie } from './types'
import axios from 'axios'

const singleMovieInitialState: SingleMovie = {
  Rated: '',
  Released: '',
  Runtime: '',
  Genre: '',
  Director: '',
  Actors: '',
  Plot: '',
  Language: '',
  Country: '',
  Awards: '',
  Ratings: [],
  Metascore: '',
  imdbRating: '',
  Title: '',
  Year: '',
  imdbID: '',
  Type: '',
  Poster: '',
}

export const getMovie = createAsyncThunk(
  'singleMovie/getMovie',
  async (id: string) => {
    console.log('start', id)
    const res = await axios.get(
      `http://www.omdbapi.com/?apikey=45f98500&i=${id}`
    )
    console.log(res)
    return res.data
  }
)

const singleMovieSlice = createSlice({
  name: 'singleMovie',
  initialState: singleMovieInitialState,
  reducers: {},
  extraReducers: (builder) => {
    builder.addCase(getMovie.fulfilled, (state, action) => {
      return action.payload
    })
  },
})

export default singleMovieSlice.reducer