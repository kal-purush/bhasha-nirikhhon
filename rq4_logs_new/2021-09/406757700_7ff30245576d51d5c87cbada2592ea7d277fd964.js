import React from "react";
import { BrowserRouter as Router, Route, Switch, Redirect } from 'react-router-dom'

import Search from "./components/search/Search.js";
import Weather from "./components/weather/Weather"

function App() {
  return (
    <Router>
      <Switch>
        <Route path="/" exact component={Search} />
        {/* <Route path="/weather" component={Weather} /> */}
      </Switch>
    </Router>
  );
}

export default App;