///<reference path="../../../typings/references.d.ts"/>
"use strict";

import GameState from "./gameState";
import * as Helpers from "./gameHelpers";
import Entity from "./entity";
import {format} from "../util/string";

export enum ActionEnum {
    Attack
}

export interface IAction {
    execute(state: GameState, actor: Entity): GameState;
}

export const actionList: { [key: number]: typeof Action } = {};

export class Action implements IAction {

    protected _actor: Entity;
    protected _target: Entity;
    protected _isPlayer: boolean;

    public execute(state: GameState, actor: Entity): GameState {

        this._actor = actor;
        if (state.player === actor) {
            this._target = state.enemy;
            this._isPlayer = true;
        } else {
            this._target = state.player;
            this._isPlayer = false;
        }

        //TODO: handle action time and validity
        let result = this.doExecute(state);

        if (this.player !== state.player) {
            result = result.setPlayer(this.player);
        }
        if (this.enemy !== state.enemy) {
            result = result.setEnemy(this.enemy);
        }

        return result;
    }

    protected get player(): Entity {
        return this._isPlayer ? this._actor : this._target;
    }

    protected get enemy(): Entity {
        return this._isPlayer ? this._target : this._actor;
    }

    protected doExecute(state: GameState): GameState {
        throw new Error("Override this in your action helper");
    }

}

export class AttackAction extends Action {

    protected doExecute(state: GameState): GameState {
        let result = state;

        if (this._target && Math.random() < this._actor.hitChance) {
            let damage = this._actor.hitDamage;
            result = Helpers.appendLog(state, format(this._actor.damageText, damage, this._target.name));
            this._target = this._target.setHealth(this._target.health - damage);
        }
        return result;
    }
}

actionList[ActionEnum.Attack] = AttackAction;