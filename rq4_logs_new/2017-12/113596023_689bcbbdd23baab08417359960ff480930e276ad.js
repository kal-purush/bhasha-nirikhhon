// @flow
import * as React from 'react';
import { HashRouter, Route, Switch } from 'react-router-dom';

import LandingPage from './components/page/LandingPage';
import DetailPage from './components/page/DetailPage';
import NotFound from './components/page/NotFound';

const App = () => {
  return (
    <HashRouter>
      <Switch>
        <Route exact path="/" component={LandingPage} />
        <Route exact path="/athlete/:id" component={DetailPage} />
        <NotFound />
      </Switch>
    </HashRouter>
  );
};

export default App;