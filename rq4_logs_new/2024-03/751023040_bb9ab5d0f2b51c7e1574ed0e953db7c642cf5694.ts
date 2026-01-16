import { Character } from "../src/classes/Character/Character";
import { CharacterStats, Stat } from "../src/classes/Character/CharacterStats";
import { Scriber } from "../src/classes/Scriber/Scriber";

let scribe = new Scriber('./files');

describe('Scriber upon Initialization', () => {
    test.todo('Scrungulus');
});

describe('Scriber saving character data', () => {
    let Euler = new Character('Euler');
    let Newton = new Character('Newton');

    Euler.stats.set(Stat.INT, Math.floor(Math.exp(1)));
    Euler.stats.set(Stat.STR, Math.floor(Math.exp(2)));
    
    scribe.saveCharacter(Euler);
    scribe.saveCharacter(Newton);

    test.todo('test validity');
});