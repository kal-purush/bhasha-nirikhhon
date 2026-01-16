import React from 'react';
import './App.css';
import Header from './components/header.js';
import Footer from './components/footer.js';
import MainDiv from './components/mainDiv.js';

function App() {
  return (
    <div className="App">
      <Header />
      <MainDiv />
      <Footer />
    </div>
  );
}

export default App;