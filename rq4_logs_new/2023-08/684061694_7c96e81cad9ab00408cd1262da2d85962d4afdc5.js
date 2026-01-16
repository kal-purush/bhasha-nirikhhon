
import './styles.css';
import Main from './main.js'
import React, {useState} from 'react'
import Header from './header.js'
import Footer from './footer.js'


function App() {



  return (
    <div className="App">
      <Header />
      <Main />
      <Footer />
    </div>
  );
}

export default App;