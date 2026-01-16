import React from 'react';
import { BrowserRouter, Route, Switch } from "react-router-dom";
import CreateNewAthlete from './components/CreateNewAthlete';
import Athletes from './components/Athletes';
import Navbar from './components/Navbar';

function App() {
  return (
    <BrowserRouter>
      <Switch>
        <Route exact path="/" component={CreateNewAthlete} />
        <Route exact path="/deportistas" component={Athletes} />
        <Route exact path="/deportista/:idValue" component={Navbar} />
      </Switch>
    </BrowserRouter>
  );
}

export default App;