import type { KnownPokemonType } from "models";

import bug from "./2/bug.png";
import electric from "./2/electric.png";
import fairy from "./2/fairy.png";
import fighting from "./2/fighting.png";
import fire from "./2/fire.png";
import flying from "./2/flying.png";
import grass from "./2/grass.png";
import ground from "./2/ground.png";
import ice from "./2/ice.png";
import normal from "./2/normal.png";
import poison from "./2/poison.png";
import psychic from "./2/psychic.png";
import rock from "./2/rock.png";
import steel from "./2/steel.png";
import water from "./2/water.png";

export const typeImageMap: Record<KnownPokemonType, string> = {
  bug: bug,
  electric: electric,
  fairy: fairy,
  fighting: fighting,
  fire: fire,
  flying: flying,
  grass: grass,
  ground: ground,
  ice: ice,
  normal: normal,
  poison: poison,
  psychic: psychic,
  rock: rock,
  steel: steel,
  water: water,
};