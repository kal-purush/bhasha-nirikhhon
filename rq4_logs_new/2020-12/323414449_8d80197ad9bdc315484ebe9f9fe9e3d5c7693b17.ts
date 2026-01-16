import nominationsSliceReducer from '../nominations/nominationSlice'
import { nominationsInitialState } from '../nominations/nominationSlice'

const finalState = {
  loading: false,
  error: '',
  nominations: [],
}

it('handles actions of type nominations/addNomination/pending', () => {
  const action = {
    type: 'nominations/addNomination/pending',
    payload: {},
  }

  const newState = nominationsSliceReducer(nominationsInitialState, action)

  expect(newState).toEqual({ ...finalState, loading: true })
})

it('handles actions of type nominations/addNomination/fulfilled', () => {
  const action = {
    type: 'nominations/addNomination/fulfilled',
    payload: {},
  }

  const newState = nominationsSliceReducer(nominationsInitialState, action)

  expect(newState).toEqual({ ...finalState, nominations: [{}] })
})

it('handles actions of type nominations/addNomination/rejected', () => {
  const action = {
    type: 'nominations/addNomination/rejected',
    payload: {},
  }

  const newState = nominationsSliceReducer(nominationsInitialState, action)

  expect(newState).toEqual({
    ...finalState,
    error: 'Failed to nominate. You may have already made this nomination.',
  })
})

it('handles actions of type nominations/getNominations/pending', () => {
  const action = {
    type: 'nominations/getNominations/pending',
    payload: {},
  }

  const newState = nominationsSliceReducer(nominationsInitialState, action)

  expect(newState).toEqual({
    ...finalState,
    loading: true,
  })
})

it('handles actions of type nominations/getNominations/fulfilled', () => {
  const action = {
    type: 'nominations/getNominations/fulfilled',
    payload: [],
  }

  const newState = nominationsSliceReducer(nominationsInitialState, action)

  expect(newState).toEqual({
    ...finalState,
    nominations: [],
  })
})

it('handles actions of type nominations/removeNomination/fulfilled', () => {
  const action = {
    type: 'nominations/removeNomination/fulfilled',
    payload: [],
  }

  const newState = nominationsSliceReducer(nominationsInitialState, action)

  expect(newState).toEqual({
    ...finalState,
    nominations: [],
  })
})

it('handles actions of type nominations/removeNomination/pending', () => {
  const action = {
    type: 'nominations/removeNomination/pending',
    payload: [],
  }

  const newState = nominationsSliceReducer(nominationsInitialState, action)

  expect(newState).toEqual({
    ...finalState,
    loading: true,
  })
})

it('handles actions of type nominations/removeNomination/rejected', () => {
  const action = {
    type: 'nominations/removeNomination/rejected',
    payload: [],
  }

  const newState = nominationsSliceReducer(nominationsInitialState, action)

  expect(newState).toEqual({
    ...finalState,
    error: 'failed to remove nomination',
  })
})

it('handles actions of unknown type', () => {
  const action = {
    type: 'purple',
    payload: [],
  }

  const newState = nominationsSliceReducer(nominationsInitialState, action)

  expect(newState).toEqual(finalState)
})