import React from 'react';
import { BrowserRouter as Router, Route, Routes } from 'react-router-dom';
import PokemonList from './PokemonList';
import PokemonDetails from './PokemonDetails'; 
import './App.css';

function App() {
  return (
    <Router>
      <Routes>
        <Route path="/" element={<PokemonList />} exact />
        <Route path="/pokemon/:id" element={<PokemonDetails />} />
      </Routes>
    </Router>
  );
}

export default App;