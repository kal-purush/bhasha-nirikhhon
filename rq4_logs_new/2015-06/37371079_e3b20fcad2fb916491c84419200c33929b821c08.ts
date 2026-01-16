"use strict";
import Scene = require("./Scene");
import Character = require("./Character");

class Line implements Scene {
  private _name: string;
  private _words: string;
  private _characters: Character[];

  get name() {
    return this._name;
  }

  get words() {
    return this._words;
  }

  get chracters() {
    return this._characters;
  }

  constructor(name: string, words: string, characters: Character[]) {
    this._name = name;
    this._words = words;
    this._characters = characters;
  }
}
export = Line;