import { createStore, applyMiddleware, compose } from 'redux'
import ReduxThunk from 'redux-thunk'
import { rootReducer, RootState } from '../reducers'

declare global {
  export interface Window {
    devToolsExtension: any
    __REDUX_DEVTOOLS_EXTENSION_COMPOSE__: any
    __PRELOADED_STATE__: RootState
  }
}

const devTools = window.devToolsExtension ? window.devToolsExtension() : (f: RootState): RootState => f

export const configureStore = (initialState?: RootState) => {
  // configure middlewares
  const middlewares = [
    ReduxThunk
  ];
  // compose enhancers
  const enhancer = compose(
    applyMiddleware(...middlewares),
    devTools
  );
  // create store
  return createStore<RootState>(
    rootReducer,
    initialState!,
    enhancer
  );
}

// pass an optional param to rehydrate state on app start
const store = configureStore();

// export store singleton instance
export default store;