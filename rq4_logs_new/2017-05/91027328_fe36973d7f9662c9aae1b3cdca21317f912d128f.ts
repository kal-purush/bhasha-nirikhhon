import Character from './character';

interface Stats {
  HP: number;
  MP: number;
  PATK: number;
  PDEF: number;
  MATK: number;
  MDEF: number;
}

interface Combatable {
  readonly stats: Stats
}

class CombatableCharacter extends Character implements Combatable {

  readonly stats: Stats
  static defaultStats = {
    HP: 1, MP: 1, PATK: 1, PDEF: 1, MATK: 1, MDEF: 1
  }

  constructor(name: string, stats?: Stats) {
    super(name);
    this.stats = stats || CombatableCharacter.defaultStats;
  }

}

export { Combatable, Stats };
export default CombatableCharacter;