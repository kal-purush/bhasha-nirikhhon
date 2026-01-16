import { MapNames } from "../../constants/MapNames.js";
import { Character } from "../../app/Character.js";
import { Skill } from "../../app/Skill.js";

export interface playerSignupDataInterface {
  username: string;
  email: string;
  password: string;
}

export interface playerStatusInterface {
  username: string;
  status: string;
}

export interface commonDataInterface {
  username: string;
}

export interface playerProfileDataInterface extends commonDataInterface {
  username: string;
  email: string;
}
interface commonCharacterDataInterface {
  username: string;
}
export interface commonGameDataInterface extends commonDataInterface {
  x: number;
  y: number;
  sprite: any;
  direction: string;
  width: number;
  height: number;
  xVelocity: number;
  yVelocity: number;
  gameObjectID: number;
  name: string;
}

export interface gameObjectDataInterface extends commonDataInterface {
  attributes: {
    level: number;
    hp: number; // Determines how many times a player can take damage before dying & hp regen amount
    sp: number; // Determines how many times a magic atk can be used and regen amount
    def: number; // Determines how much damage is taken from phyiscal hits
    mdef: number; // Determines how much damage is taken from Magic hits
    crit: number; // Determines wheter or not a hit does increased damgage & increased damage amount

    //stats directly controlled by the player when using levelinh points
    Atk: number; // Determines Physical atk damage and gives a minor boost to hp total
    Matk: number; // Determines Magic atk damage and gives a minor boost to sp total
    Vit: number; // Increases hp and def
    Men: number; //Increases sp & mdef
    Dex: number; //Increases Crit
  };
}

export interface CharacterCreationDataInterface
  extends commonGameDataInterface {
  characterGender: string;
  player: any;
  location: MapNames;
}
export interface characterDataInterface extends CharacterCreationDataInterface {
  attributes: {
    level: number;
    experience: number;
    experienceCap: number;
    statPoints: number;
    hp: number; // Determines how many times a player can take damage before dying & hp regen amount
    hpCap: number;
    sp: number; // Determines how many times a magic atk can be used and regen amount
    spCap: number;
    def: number; // Determines how much damage is taken from phyiscal hits
    mdef: number; // Determines how much damage is taken from Magic hits
    crit: number; // Determines wheter or not a hit does increased damgage & increased damage amount

    //stats directly controlled by the player when using levelinh points
    Atk: number; // Determines Physical atk damage and gives a minor boost to hp total
    Matk: number; // Determines Magic atk damage and gives a minor boost to sp total
    Vit: number; // Increases hp and def
    Men: number; //Increases sp & mdef
    Dex: number; //Increases Crit
  };
  unlockedSkills: Skill[];
  class: string;
  guild: string;
  friends: string[];
  items: string[];
  hotbar: any[];
  equipment: {
    head: number[] | null;
    chest: number[] | null;
    legs: number[] | null;
    weapon: number[] | null;
  };
}

export interface inputHistory {
  location: { x: number; y: number };
  tick: number;
  confirmedPosition: boolean;
}
export default interface playerDataInterface
  extends playerProfileDataInterface {
  characters: [characterDataInterface];
  playerID: string;
}