import { useEffect, useState } from "react";
import { Pokemon } from "../models";
import { getPokemonTypeDamage } from '../models';

export const usePokemonWeaknesses = (pokemon: Pokemon | undefined) => {
  const [weaknesses, setWeaknesses] = useState<string[]>([]);

  useEffect(() => {
    const fetchWeaknesses = async () => {
      if (!pokemon) return; // Si pokemon es undefined, no hagas nada
      const weaknessesResult = await PokemonDetails({ pokemon });
      setWeaknesses(weaknessesResult);
    };

    fetchWeaknesses();
  }, [pokemon]);

  return weaknesses;
};

const PokemonDetails = async ({ pokemon }: { pokemon: Pokemon }) => {
    const weaknesses: string[] = [];
    const doubleDamageFrom: { name: string }[] = [];
    const halfDamageFrom: { name: string }[] = [];
  
    // Obtener las debilidades y resistencias de ambos tipos
    for (const type of pokemon.types) {
      const pokemonTypeDamage = await getPokemonTypeDamage(type.type.url);
      doubleDamageFrom.push(...pokemonTypeDamage.damage_relations.double_damage_from);
      halfDamageFrom.push(...pokemonTypeDamage.damage_relations.half_damage_from);
    }
  
    // Filtrar las debilidades que no son neutralizadas por las resistencias del otro tipo
    for (const weakness of doubleDamageFrom) {
      if (!halfDamageFrom.some(resistance => resistance.name === weakness.name)) {
        weaknesses.push(weakness.name);
      }
    }
  
    return weaknesses;
  };  