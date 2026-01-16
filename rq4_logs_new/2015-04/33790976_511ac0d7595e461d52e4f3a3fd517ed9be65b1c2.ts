/**
 * Created by dhnishi on 4/25/15.
 */
/// <reference path="../_all.ts"/>

module battlejack {
    export class EndBattleService {
        private battleEntityService : BattleEntitiesService;
        private playerEntityService : PlayerEntityService;

        constructor(battleEntitiesService : BattleEntitiesService, playerEntityService : PlayerEntityService) {
            this.battleEntityService = battleEntitiesService;
            this.playerEntityService = playerEntityService;
        }

        didPlayerWin() {
            this.playerEntityService.entities.map((entity : Entity) => {
               if (entity.isIncapacitated()) {
                   return false;
               }
            });
            return true;
        }

        cleanup() {
            this.playerEntityService.entities.map((entity : Entity) => {
                // TODO: Replace this with a real value.
                entity.stats.hp = 10;
            });
        }

        finishBattle() {
            if (this.didPlayerWin()) {
                console.log("YEAH");
            }
            this.cleanup();
        }
    }
}