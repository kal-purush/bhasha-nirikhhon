import React, { Component } from 'react';
import Menu from './components/Menu'
import Content from './components/Content'
import Header from './components/Header'
import Footer from './components/Footer'

class App extends Component {
  render() {
    return (
      <div class="container">
      <div class="header">
      <Header />
      </div>
     <div class="menu">
        <Menu />
        </div>
        <div class="content">
        <Content />
        </div>
        <div class="footer">
        <Footer />
        </div>
      </div>
    );
  }
}

export default App;