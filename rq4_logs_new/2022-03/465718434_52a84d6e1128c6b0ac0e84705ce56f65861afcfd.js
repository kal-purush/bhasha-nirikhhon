import React from 'react';
import './App.css';
import {BrowserRouter as Router,Routes,Route} from 'react-router-dom'
import Navbar from './components/Navbar'
import Home from './components/Home';
import Thali from './components/Thali';
import Cart from './components/Cart';
import Product from './components/Product';
import Footer from './components/Footer';
import './App.css';
function App() {
  return (
    <div className="App">
     <Router>
       <Navbar/>
       <Routes>
       <Route path="/" element={<Thali/>} />
       <Route path="/Thali" element={<Thali />} />
       <Route path='/Product' element={<Product/>}/>
       <Route path="/Cart" element={<Cart />} />
       </Routes>
       <Footer/>
     </Router>
    </div>
  );
}

export default App;