/**
 * Created by dhnishi on 4/14/15.
 */

/// <reference path="_all.ts"/>

module battlejack {

    /**
     * A BattleAction defines an attacker, the targets of the attack, and a function which mutates them appropriately.
     * This encapsulates the entire action of attacking.
     */
    export class BattleAction {
        entity : EntityInBattle;
        targets : EntityInBattle[];
        priority : number;
        mutateTargets : (targets : EntityInBattle[], entity : EntityInBattle) => void;
    }
}