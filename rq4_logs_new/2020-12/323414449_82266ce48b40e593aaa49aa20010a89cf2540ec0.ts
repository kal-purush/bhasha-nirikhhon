import movieSliceReducer from '../movies/moviesSlice'
import { moviesInitialState } from '../movies/moviesSlice'

it('handles actions of type movies/searchMovieDatabase/pending', () => {
  const action = {
    type: 'movies/searchMovieDatabase/pending',
    payload: {
      movies: [],
      string: 'hey',
    },
  }

  const newState = movieSliceReducer(moviesInitialState, action)

  expect(newState).toEqual({
    searchString: '',
    movies: [],
    loading: true,
  })
})

it('handles actions of type movies/searchMovieDatabase/rejected', () => {
  const action = {
    type: 'movies/searchMovieDatabase/rejected',
    payload: {
      movies: [],
      string: 'hey',
    },
  }

  const newState = movieSliceReducer(moviesInitialState, action)

  expect(newState).toEqual({
    searchString: '',
    movies: [],
    loading: false,
  })
})

it('handles actions of type movies/searchMovieDatabase/fulfilled', () => {
  const action = {
    type: 'movies/searchMovieDatabase/fulfilled',
    payload: {
      movies: [],
      string: 'hey',
    },
  }

  const newState = movieSliceReducer(moviesInitialState, action)

  expect(newState).toEqual({
    searchString: 'hey',
    movies: [],
    loading: false,
  })
})