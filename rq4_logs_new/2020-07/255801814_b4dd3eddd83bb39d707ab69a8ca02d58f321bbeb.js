import React, { Component } from 'react';
import { BrowserRouter as Router, Route, Switch } from 'react-router-dom';
import './App.css';

// Material UI Components
import CssBaseline from '@material-ui/core/CssBaseline';

//Components
import Navbar from './components/Navbar';

// Pages
import Home from './pages/home';
import Login from './pages/login';
import SignUp from './pages/signup';

export class App extends Component {
  render() {
    return (
      <Router>
        <CssBaseline />
        <Navbar />
        <div className="container">
          <Switch>
            <Route exact path="/" component={Home} />
            <Route exact path="/login" component={Login} />
            <Route exact path="/signup" component={SignUp} />
          </Switch>
        </div>
      </Router>
    );
  }
}

export default App;