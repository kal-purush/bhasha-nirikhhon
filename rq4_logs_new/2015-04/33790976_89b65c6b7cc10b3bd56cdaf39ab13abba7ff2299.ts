/**
 * Created by dhnishi on 4/19/15.
 */
/// <reference path="_all.ts"/>

module battlejack {
    export class ArenaBattleFactory {
        // TODO: Make a constructor that allows this to make different types of enemies.
        generateBattle(difficulty) {
            var enemies = [];
            for (var enemyNum = 1; enemyNum <= 4; enemyNum++) {
                enemies.push(this.generateEnemy(difficulty));
            }
            return enemies;
        }

        generateEnemy(difficulty) {
            // TODO: Make this change based upon difficulty.
            return new CPUEntityInBattle(new Entity, new Hand([]));
        }
    }
}