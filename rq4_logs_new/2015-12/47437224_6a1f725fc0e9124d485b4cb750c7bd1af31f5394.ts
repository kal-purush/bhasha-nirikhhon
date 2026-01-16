import {
  it,
  expect,
  describe
} from 'angular2/testing';

import {Player} from './player_model.js';

describe('player_model', () => {
  describe('creation', () => {
    it('should create the player correctly', () => {
      let _p = new Player();

      expect(_p).toBeDefined();
      expect(_p.score).toBe(0);
    });
  });

  describe('wins', () => {
    it('should add 1 to the score', () => {
      let _p = new Player();

      _p.wins();

      expect(_p.score).toBe(1);

      _p.wins();

      expect(_p.score).toBe(2);
    });
  });
});