///<reference path="../../../typings/references.d.ts"/>
"use strict";
import Model from "./model";
import * as Immutable from "immutable";

export default class GameState extends Model {

    public get time(): number {
        return this._data.get("time");
    }

    public setTime(time: number): GameState {
        return this.set("time", time);
    }

    private set(name: string, value: any): GameState {
        return this.setValue(GameState, name, value);
    }

}

export const defaultState = new GameState(Immutable.Map({
    time: 0
}));