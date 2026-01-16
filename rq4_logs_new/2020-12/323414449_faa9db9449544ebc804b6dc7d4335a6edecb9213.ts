import singleMovieSliceReducer, {
  singleMovieInitialState,
} from '../singleMovie/singleMovieSlice'

const finalState = {
  singleMovie: {},
  loading: false,
}

it('handles actions of type singleMovie/getMovie/pending', () => {
  const action = {
    type: 'singleMovie/getMovie/pending',
    payload: {},
  }

  const newState = singleMovieSliceReducer(singleMovieInitialState, action)

  expect(newState).toEqual({ ...singleMovieInitialState, loading: true })
})

it('handles actions of type singleMovie/getMovie/fulfilled', () => {
  const action = {
    type: 'singleMovie/getMovie/fulfilled',
    payload: {},
  }

  const newState = singleMovieSliceReducer(singleMovieInitialState, action)

  expect(newState).toEqual({ singleMovie: {}, loading: false })
})