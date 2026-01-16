// Package imports
import { createStore, applyMiddleware, compose } from "redux";
import ReduxThunk from "redux-thunk";
// Non-package imports
import rootReducer from "redux/reducers";

// Redux does not provide types for this function as it is exposed by Redux DevTools
declare global {
  interface Window {
    __REDUX_DEVTOOLS_EXTENSION_COMPOSE__?: typeof compose;
  }
}

const composeEnhancers = window.__REDUX_DEVTOOLS_EXTENSION_COMPOSE__ || compose;

// create the redux store to be used by the Provider in index.js
export default createStore(
  rootReducer,
  composeEnhancers(applyMiddleware(ReduxThunk))
);