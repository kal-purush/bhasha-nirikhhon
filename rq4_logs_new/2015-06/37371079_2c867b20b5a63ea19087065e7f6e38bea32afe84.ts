"use strict";
import Scene = require("./Scene");
import Character = require("./Character");
import Choice = require("./Choice");

class Choices implements Scene {
  private _background: string;
  private _characters: Character[];
  private _choices: Choice[];
    
  background(defaultValue: string) {
    if (this._background) {
      return this._background;
    } else {
      return defaultValue;
    }
  }
  
  getImages() {
    if (this._background) {
      let xs = this._characters.map((c: Character) => c.image);
      xs.unshift(this._background);
      return xs;
    } else {
      return this._characters.map((c: Character) => c.image);
    }
  }
  
  get characters() {
    return this._characters;
  }
  
  get choices() {
    return this._choices;
  }
  
  constructor(background: string, characters: Character[], choices: Choice[]) {
    this._background = background;
    this._characters = characters;
    this._choices = choices;
  }
}
export = Choices;