import React from 'react';
import "./index.css";
import Home from './Routes/Home';
import Contact from './Routes/Contact';
import Project from './Routes/Project';
import About from './Routes/About';
import { Routes, Route } from 'react-router-dom';
import Coming from './Routes/Coming';
import { Spring } from 'react-spring';

function App() {
  return (
    <>
    <Routes>
      <>
      <Route path="/" element={<Home/>} />
      <Route path="/projects" element={<Project/>} />
      <Route path="/about" element={<About/>} />
      <Route path="/contact" element={<Contact/>} />
      <Route path="/coming" element={<Coming />}/>
      </>
    </Routes>
    </>
  );
}

export default App;