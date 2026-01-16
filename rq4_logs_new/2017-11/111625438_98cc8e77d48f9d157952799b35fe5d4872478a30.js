import React, { Component } from 'react';
import logo from './logo.svg';
import './App.css';
import design from './icons/design.svg';

const trackIcons = {
  beginner: require('./icons/design.svg'),
};

const Icon = (props) =>
  <img src={require(`./icons/${props.track}.svg`)} />;

const Difficulty = (props) =>
  <img src={require(`./icons/${props.difficulty}.svg`)} />;

class App extends Component {
  render() {
    return (
      <div className="App">
        <header className="App-header">
          <img src={logo} className="App-logo" alt="logo" />
          <h1 className="App-title">Welcome to React</h1>
        </header>
        <div className="list">
          <ul className="list-items">
            <li className="list-item">
              <Icon track="design" />
              <Difficulty difficulty="intermediate" />
              <h3>Suspendisse eleifend, leo eget.</h3>
            </li>
            <li className="list-item">
              <Icon track="modules" />
              <Difficulty difficulty="beginner" />
              <h3>Nunc at scelerisque nulla. Morbi vel.</h3>
            </li>
            <li className="list-item">
              <Icon track="theming" />
              <Difficulty difficulty="expert" />
              <h3>Nunc at scelerisque nulla. Morbi vel.</h3>
            </li>
          </ul>
        </div>
      </div>
    );
  }
}

export default App;