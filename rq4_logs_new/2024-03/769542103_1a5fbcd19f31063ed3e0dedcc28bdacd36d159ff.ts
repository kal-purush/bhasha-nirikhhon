import { useState, useEffect } from 'react';
import { Pokemon } from "../models";

export interface FavoriteProps {
  selectedPokemon: Pokemon | null;
}

export interface FavoritePokemon {
  name: string;
  isFavorite: boolean;
}

export const useFavorite = (selectedPokemon: Pokemon | null) => { // Recibe selectedPokemon como argumento
  const [favoritePokemon, setFavoritePokemon] = useState<FavoritePokemon[]>([]);
  const [errorMessage, setErrorMessage] = useState<string>("");

  const toggleFavorite = (selectedPokemon: Pokemon | null) => {
    if (!selectedPokemon) return;

    const pokemonIndex = favoritePokemon.findIndex(p => p.name === selectedPokemon.name);
    if (pokemonIndex !== -1) {
      // Si el Pokémon ya está en favoritos, lo eliminamos
      const updatedFavoritePokemon = [...favoritePokemon];
      updatedFavoritePokemon.splice(pokemonIndex, 1);
      setFavoritePokemon(updatedFavoritePokemon);
      console.log(`El Pokémon ${selectedPokemon.name} ha sido eliminado de favoritos.`);
    } else {
      // Si el Pokémon no está en favoritos, lo añadimos
      if (favoritePokemon.length < 6) {
        setFavoritePokemon([...favoritePokemon, { name: selectedPokemon.name, isFavorite: true }]);
        console.log(`El Pokémon ${selectedPokemon.name} ha sido añadido a favoritos.`);
      } else {
        // Si ya hay 6 Pokémon favoritos, mostramos un mensaje de error durante 5 segundos
        setErrorMessage("No se pueden agregar más de 6 Pokémons a favoritos.");
        console.log("No se pueden agregar más de 6 Pokémons a favoritos.");
      }
    }
  };

  useEffect(() => {
    console.log("Lista de Pokémon favoritos:", favoritePokemon.map(p => p.name));

    // Limpiar el mensaje de error después de 5 segundos
    if (errorMessage) {
      const timer = setTimeout(() => {
        setErrorMessage("");
      }, 3000);

      return () => {
        clearTimeout(timer);
        setErrorMessage("");
      };
    }
  }, [favoritePokemon, errorMessage, selectedPokemon]); // Agrega selectedPokemon aquí

  return { favoritePokemon, toggleFavorite, errorMessage };
};