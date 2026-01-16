import { tv } from "tailwind-variants";

export const pokemonCardStyles = tv({
  slots: {
    pokemonCardWrapper:
      "flex flex-col justify-between h-108 w-104 rounded-8 shadow hover:cursor-pointer",
    pokemonNumberWrapper: "flex justify-end h-12 w-full py-4 px-8 text-xs",
    pokemonImageWrapper: "relative",
    pokemonImage: "absolute top-1/2 left-1/2 transform-translate-xy-50-50",
    pokemonNameWrapper:
      "text-center py-24 px-8 pb-4 bg-grayscale-background text-xs overflow-hidden whitespace-nowrap overflow-ellipsis",
  },
});