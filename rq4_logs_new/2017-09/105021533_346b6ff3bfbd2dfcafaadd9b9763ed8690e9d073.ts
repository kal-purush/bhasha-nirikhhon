export const INCREMENT_COUNTER = 'INCREMENT_COUNTER'
export const DECREMENT_COUNTER = 'DECREMENT_COUNTER'

export type Actions = {
  INCREMENT_COUNTER: {
    type: typeof INCREMENT_COUNTER
  },
  DECREMENT_COUNTER: {
    type: typeof DECREMENT_COUNTER
  }
}

// Action Creators
export const actionCreators = {
  incrementCounter: (): Actions[typeof INCREMENT_COUNTER] => ({
    type: INCREMENT_COUNTER
  }),
  decrementCounter: (): Actions[typeof DECREMENT_COUNTER] => ({
    type: DECREMENT_COUNTER
  })
}