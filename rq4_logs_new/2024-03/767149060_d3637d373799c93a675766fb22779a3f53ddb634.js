import React from 'react';
import './App.css';
import { NavBar } from './components/NavBar.js';
import { Banner } from './components/Banner.js'
import { Skills } from './components/Skills.js'
import { Projects } from './components/Projects.js'
import 'bootstrap/dist/css/bootstrap.min.css'

/*copy this when react not launching
export NODE_OPTIONS=--openssl-legacy-provider*/
function App() {
  return (
    <div className="App">
      <NavBar />
      <Banner />
      <Skills />
      <Projects />
    </div>
  );
}

export default App;