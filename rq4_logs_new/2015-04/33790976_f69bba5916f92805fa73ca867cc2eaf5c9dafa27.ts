/**
 * Created by dhnishi on 4/17/15.
 */
/// <reference path="_all.ts"/>

module battlejack {
    export class CharacterCreator {
        stats : StatBlock;
        pointsRemaining : number;
        CHARACTER_CREATION_POINTS = 32;
        POINT_BUY_COSTS = {
            8: 1,
            9: 1,
            10: 2,
            11: 3,
            12: 4,
            13: 5,
            14: 6,
            15: 8,
            16: 10,
            17: 13,
            18: 16
        };
        MIN_STAT = 8;
        MAX_STAT = 18;

        constructor() {
            this.stats = new StatBlock();
            this.pointsRemaining = this.CHARACTER_CREATION_POINTS;
        }

        incrementStat(stat : Stat) {
            var currentValue = this.stats.getStat(stat);
            var cost = this.POINT_BUY_COSTS[currentValue]

            if (cost > this.pointsRemaining) {
                return;
            }
            if (currentValue >= this.MAX_STAT) {
                return;
            }
            this.pointsRemaining -= cost;
            this.stats.setStat(stat, currentValue + 1);
        }

        decrementStat(stat : Stat) {
            var currentValue = this.stats.getStat(stat);
            if (currentValue <= this.MIN_STAT) {
                return;
            }
            this.pointsRemaining += this.POINT_BUY_COSTS[currentValue - 1];
            this.stats.setStat(stat, currentValue - 1);
        }

        createCharacter(name : string) {
            var newEntity = new Entity(name);
            newEntity.stats = this.stats;
            console.log(newEntity);
            return newEntity;
        }

    }
}