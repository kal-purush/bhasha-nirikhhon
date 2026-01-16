import React, { Component } from 'react';
import RootContainer from './components/RootContainer';
import './App.css';

import {Provider} from 'react-redux';
import store from './store';

class App extends Component {

  render() {
    return (
        <Provider store={store} >
          <RootContainer />
        </Provider>
    );
  }
}

export default App;