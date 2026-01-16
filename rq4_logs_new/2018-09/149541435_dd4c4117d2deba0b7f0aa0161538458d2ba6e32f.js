import React, { Component } from 'react';
import './App.css';
import Header from "./components/Header";
import Section from "./components/Section";
import Navbar from "./components/Navbar";
import Footer from "./components/Footer";


class App extends Component {
  render() {
    return (
      <div>
      <Navbar />
      <Header />
      <Section />
      <Footer />
    </div>
    );
  }
}

export default App;