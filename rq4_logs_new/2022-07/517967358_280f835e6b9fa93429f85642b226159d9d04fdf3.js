import React from "react"
import { BrowserRouter as Router, Switch, Route, Link } from 'react-router-dom'
import Home from './components/index.js'

function App() {
  return (
    <div className="App">
     
     <Router>
        <Switch>
          <Route exact path="/" ><Home /></Route>
        
        </Switch>

      </Router>
    </div>
  );
}

export default App;