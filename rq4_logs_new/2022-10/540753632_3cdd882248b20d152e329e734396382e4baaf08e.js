import React from "react";
import styles from "./App.module.scss";
import "./App.scss";
import { Routes, Route } from "react-router-dom";
import Menu from "./components/Menu/Menu";
import PokemonList from "./components/PokemonList/PokemonList";
import TypeList from "./components/TypeList/TypeList";

const App = () => {
  return (
    <div className={styles.container}>
      <div className={styles.header}>
        <h1>Pokedex</h1>
        <Menu />
      </div>
      <div className={styles.content}>
        <Routes>
          <Route path="/" element={<TypeList />} />
          <Route path="/pokemons" element={<PokemonList />} />
        </Routes>
      </div>
    </div>
  );
};

export default App;