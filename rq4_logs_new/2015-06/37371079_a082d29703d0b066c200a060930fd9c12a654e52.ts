"use strict";
class IndentContext {
  private _levels: number[];
  private _current: number;
  private _new: number;

  get levels() {
    return this._levels;
  }

  get currentLevel() {
    return this._current;
  }

  get newLevel() {
    return this._new;
  }

  constructor(levels: number[], current: number, newLevel: number) {
    this.levels = levels;
    this._current = current;
    this._new = newLevel;
  }

  unshiftLevel(level: number): IndentContext {
    let levels = this._levels.slice();
    levels.unshift(level);
    return new IndentContext(levels, this._current, this._new);
  }

  updateLevels(levels: number[]): IndentContext {
    return new IndentContext(levels, this._current, this._new);
  }

  updateCurrentLevel(currentLevel: number): IndentContext {
    return new IndentContext(this._levels, currentLevel, this._new);
  }

  updateNewLevel(newLevel: number): IndentContext {
    return new IndentContext(this._levels, this._current, newLevel);
  }
}
export = IndentContext;