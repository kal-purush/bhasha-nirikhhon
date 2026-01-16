import logo from './logo.svg';
import React from 'react';
import BookingForm from './BookingForm';
import './App.css';

function App() {
  return (
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
  );
}
function App() {
  return (
    <div className="App">
      <h1>Little Lemon Restaurant</h1>
      <BookingForm />
    </div>
  );
}

export default App;