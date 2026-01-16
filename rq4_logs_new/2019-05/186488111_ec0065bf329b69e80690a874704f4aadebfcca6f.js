import React from 'react';
import logo from './logo.svg';
import './App.css';
import Clock from './components/Clock'

function App() {
  return (
    <div className="App">
      <header className="App-header">
        <img src={logo} className="App-logo" alt="logo" />
        <h1><Clock/></h1>
        
      </header>
    </div>
  );
}

export default App;