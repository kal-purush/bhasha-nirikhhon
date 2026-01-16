import React, { Component } from 'react';
import logo from './logo.svg';
import './App.css';
import events from './events';

import BigCalendar from 'react-big-calendar';
import 'react-big-calendar/lib/css/react-big-calendar.css';
import Calendar from './basic'
import './style.css'

class App extends Component {
  hello(user){
    return "Good morning " + user.firstName + " " + user.lastName;
  }  
  render() {
    return (
      <div className="App">
        <div className="App-header">
          <img src={logo} className="App-logo" alt="logo" />
          <h2>Welcome to React</h2>
        </div>
        <p className="App-intro">
          To get started, edit <code>src/App.js</code> and save to reload.
        </p>
        <div id="container">
        	<Calendar className="calendar"/>
        </div>
      </div>
    );
  }
}

export default App;