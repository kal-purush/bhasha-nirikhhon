
import 'babel-polyfill';
import React from 'react';
import ReactDOM from 'react-dom';
import {Provider} from 'react-redux';
import configureStore from './configureStore';
import {Router, browserHistory} from 'react-router';
require('styles/app.scss');
import router from './router';
const initialState = window.__INITIAL_STATE__;
const store = configureStore(initialState, window);
const rootElement = document.getElementById('app');


ReactDOM.render(
  <Provider store={store}>
    <Router history={browserHistory} routes={router} />
  </Provider>
, rootElement);