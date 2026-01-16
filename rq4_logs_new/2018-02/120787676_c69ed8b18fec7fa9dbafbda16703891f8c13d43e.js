import React, { Component } from 'react';
import Nav from './components/nav/Nav.js';
import List from './components/list/List.js';
import './App.css';

class App extends Component {
  render() {
    return (
      <div className="App">
        <Nav></Nav>
        <List></List>
      </div>
    );
  }
}

export default App;