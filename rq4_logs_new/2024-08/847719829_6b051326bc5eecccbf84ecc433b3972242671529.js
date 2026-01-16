import React from 'react';
import Navbar from './navbar';
import Body from './body';
import Footer from './footer';
import './App.css'; // Main CSS file to import all styles

function App() {
  return (
    <div>
      <Navbar />
      <Body />
      <Footer />
    </div>
  );
}

export default App;