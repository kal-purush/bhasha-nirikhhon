import React from 'react';
import MainView from './view/MainView.js';
import HUD from './hud/HUD.js';
import './App.css';

class App extends React.Component {
  render() {
    return (
      <div className="app fullscreen" >
        <MainView />
        <HUD />
      </div>
    )
  }
}

export default App;