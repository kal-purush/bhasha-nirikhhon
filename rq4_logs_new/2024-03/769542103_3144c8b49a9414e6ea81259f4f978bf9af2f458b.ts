import { useEffect, useState } from "react";

import type { PokemonUri } from "models";

const API_ENDPOINT = "https://pokeapi.co/api/v2/pokemon/";

type UsePokemonOpts = {
  limit?: number;
};

export function usePokemonList({ limit }: UsePokemonOpts = { limit: 42 }) {
  const [isLoading, setIsLoading] = useState(true);
  const [pokemonList, setPokemonList] = useState<PokemonUri[]>([]);

  useEffect(() => {
    const fetchPokemonList = async () => {
      try {
        const response = await fetch(`${API_ENDPOINT}?limit=${limit}`);
        const data = await response.json();
        setPokemonList(data.results);
      } catch (error) {
        console.error("Error fetching Pokemon list:", error);
      } finally {
        setIsLoading(false);
      }
    };

    fetchPokemonList();
  }, [limit]);

  return { pokemonList, isLoading };
}