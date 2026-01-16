import React, { Component } from 'react';
import { Grommet } from 'grommet';
import { BrowserRouter } from 'react-router-dom';
import { Route } from 'react-router';

import Home from './Home';

const theme = {
  global: {
    font: {
      family: 'Roboto',
      size: '14px',
      height: '20px',
    }
  },
};

class App extends Component {
  render() {
    return (
      <Grommet theme={theme}>
        <BrowserRouter>
          <Route path="/" component={Home}/>
        </BrowserRouter>
      </Grommet>
    );
  }
}

export default App;