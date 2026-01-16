import React from 'react';
import { BrowserRouter as Router, Route, Switch } from 'react-router-dom';
import './App.css';
import Main from './pages/main/Main';
import Login from './pages/login/Login'

class App extends React.Component {
  render(){
    return (
      <Router>
        <Switch>
          <Route path="/" component={Login} exact={true} />
          <Route path="/main" component={Main} />
        </Switch>
      </Router>
    );
  }
}

export default App;

{/*
    <div className="App">
      <header className="App-header">
        <img src={logo} className="App-logo" alt="logo" />
        <p>
          Edit <code>src/App.js</code> and save to reload.
        </p>
        <a
          className="App-link"
          href="https://reactjs.org"
          target="_blank"
          rel="noopener noreferrer"
        >
          Learn React
        </a>
      </header>
    </div>
*/}