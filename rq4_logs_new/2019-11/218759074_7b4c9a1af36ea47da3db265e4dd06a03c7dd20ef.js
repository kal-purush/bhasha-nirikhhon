// define app store actions names
export const ACTION_APP_INCREMENT = 'ActionAppIncrement'
export const ACTION_APP_DECREMENT = 'ActionAppDecrement'

// define app store mutations names
const INCREMENT_VALUE = 'IncrementValue'
const DECREMENT_VALUE = 'DecrementValue'

// initial app state
const state = {
  counter: 0
}

// to retrieve data from store
const getters = {
  getCounter (state) {
    return state.counter
  }
}

// app store actions
// dispatching later to store in components, in order to perform some actions related with updating state
const actions = {
  [ACTION_APP_INCREMENT] (context) {
    context.commit(INCREMENT_VALUE);
  },
  [ACTION_APP_DECREMENT] (context) {
    context.commit(DECREMENT_VALUE);
  }
}

// app store mutations
// To update state values, called by actions
const mutations = {
  [INCREMENT_VALUE] (state) {
    state.counter = state.counter + 1
  },
  [DECREMENT_VALUE] (state) {
    state.counter = state.counter -1
  }
}

export default {
  state,
  actions,
  mutations,
  getters
}