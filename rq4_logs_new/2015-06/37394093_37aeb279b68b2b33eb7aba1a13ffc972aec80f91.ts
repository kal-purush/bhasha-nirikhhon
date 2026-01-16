///<reference path="../../../typings/references.d.ts"/>
"use strict";
import Model from "./model";

export default class Entity extends Model {

    public get name(): string {
        return this._data.get("name", "Nameless");
    }

    public setName(name: string): Entity {
        return this.set("name", name);
    }

    public get health(): number {
        return this._data.get("health", 0);
    }

    public setHealth(health: number): Entity {
        // stay within 0 - maxHealth
        health = Math.max(health, 0);
        health = Math.min(health, this.maxHealth);
        return this.set("health", health);
    }

    public get maxHealth(): number {
        return this._data.get("maxHealth", 0);
    }

    public setMaxHealth(health: number): Entity {
        health = Math.max(health, 0);
        let result = this.set("maxHealth", health);
        // ensure that health stays less than maxHealth
        if (result.health > result.maxHealth) {
            result = result.setHealth(result.maxHealth);
        }
        return result;
    }

    public get IsAlive(): boolean {
        return this.health > 0;
    }

    protected set(name: string, value: any): Entity {
        return this.setValue(Entity, name, value);
    }

}