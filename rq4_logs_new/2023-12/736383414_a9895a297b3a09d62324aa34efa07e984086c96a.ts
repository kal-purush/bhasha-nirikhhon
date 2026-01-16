import { tv } from "tailwind-variants";

export const detailsPokemonPageStyles = tv({
  slots: {
    detailsPokemonWrapper:
      "flex flex-col h-screen p-[4px] w-screen shadow-md bg-gray-300",
  },

  variants: {
    backgroundTypeColor: {
      bug: {
        detailsPokemonWrapper: "bg-pokemonType-bug",
      },
      dark: {
        detailsPokemonWrapper: "bg-pokemonType-dark",
      },
      dragon: {
        detailsPokemonWrapper: "bg-pokemonType-dragon",
      },
      electric: {
        detailsPokemonWrapper: "bg-pokemonType-electric",
      },
      fairy: {
        detailsPokemonWrapper: "bg-pokemonType-fairy",
      },
      fighting: {
        detailsPokemonWrapper: "bg-pokemonType-fighting",
      },
      fire: {
        detailsPokemonWrapper: "bg-pokemonType-fire",
      },
      flying: {
        detailsPokemonWrapper: "bg-pokemonType-flying",
      },
      ghost: {
        detailsPokemonWrapper: "bg-pokemonType-ghost",
      },
      normal: {
        detailsPokemonWrapper: "bg-pokemonType-normal",
      },
      grass: {
        detailsPokemonWrapper: "bg-pokemonType-grass",
      },
      ground: {
        detailsPokemonWrapper: "bg-pokemonType-ground",
      },
      ice: {
        detailsPokemonWrapper: "bg-pokemonType-ice",
      },
      poison: {
        detailsPokemonWrapper: "bg-pokemonType-poison",
      },
      psychic: {
        detailsPokemonWrapper: "bg-pokemonType-psychic",
      },
      rock: {
        detailsPokemonWrapper: "bg-pokemonType-rock",
      },
      steel: {
        detailsPokemonWrapper: "bg-pokemonType-steel",
      },
      water: {
        detailsPokemonWrapper: "bg-pokemonType-water",
      },
      type: {
        detailsPokemonWrapper: "grayscale.wireframe",
      },
    },
  },
});