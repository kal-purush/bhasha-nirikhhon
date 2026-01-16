import { CharacterStats } from "./CharacterStats";

export class Character {
    readonly name: String;
    stats: CharacterStats = new CharacterStats();
    isAlive: boolean = true;

    constructor(name: String, stats?: CharacterStats) {
        this.name = name;
        this.stats = stats ?? this.stats;
    }
    
    addStats(stats: CharacterStats) {
        this.stats.combine(stats);
        return this;
    }

    scaleStats(scalar: number) {
        this.stats.scale(scalar);
        return this;
    }
}