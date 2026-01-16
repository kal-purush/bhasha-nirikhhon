import React from "react";

import './styles/App.css';

import { withAuthentication } from "./api/Sessions";
import { withFirebase } from "./api/Firebase";
import { compose } from "recompose";



const AppBase = ({firebase}) => (
  <div className="App">
    <header className="App-header">
      <p>
        Example App Wrapped With Firebase!!
      </p>
	  <p>
	    Firebase is {firebase ? "enabled" : "disabled"}
	  </p>
    </header>
  </div>
);



const App = compose(withFirebase, withAuthentication)(AppBase);

export default App;