/// <reference path="../AbstractEffect.ts" />

class NonlethalDamageEffect extends AbstractEffect {

    damage: number;
    constructor(damage: number) {
        super();
        this.damage = damage;
    }

    effect(user: Combatant, target: Combatant): void {
        let oldHealth = target.health;

        // check if we can damage normally
        let clone = target.clone();

        clone.wound(user.statusFold(StatusFolds.DAMAGE_DEALT, this.damage));

        if (clone.health > 0) {
            // deal damage normally
            target.wound(user.statusFold(StatusFolds.DAMAGE_DEALT, this.damage));
        } else {
            // deal the exact right amount of damage to bring health to 1, bypassing defenses
            target.directDamage(target.health - 1);
        }

        if (user instanceof Player) {
            Game.currentRun.addStatistic(RunStatistics.DAMAGE_DEALT, oldHealth - target.health);
        }
    }

    toString(): string {
        return `do up to ${this.damage} nonlethal damage`;
    }

    clone(): NonlethalDamageEffect {
        return new NonlethalDamageEffect(this.damage);
    }

}