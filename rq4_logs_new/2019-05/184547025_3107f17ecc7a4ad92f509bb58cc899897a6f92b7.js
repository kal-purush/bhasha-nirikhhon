import React, { Component } from 'react';
import './App.css';
import { BrowserRouter as Router, Route } from 'react-router-dom'
import Home from './components/Home';
import About from './components/About';
import Shop from './components/Shop1';
import Contact from './components/Contact';
import Product from './components/Product';
import MainNavigation from './components/MainNavigation';



class App extends Component {
  render() {
    return (
      <Router>
        <div>
          <MainNavigation />
          <Route exact path="/" component={Home} />
          <Route path="/about" component={About} />
          <Route path="/shop1" component={Shop} />
          <Route path="/contact" component={Contact} />
          <Route path="/product/:id" component={Product} />

        </div>

      </Router>  
    )

  }
}  
 
export default App;