import Player from '../src/Player.js';
import Game from '../src/Game.js';

import chai from 'chai';
const expect = chai.expect;

describe('Player', function() {
let game; 

  beforeEach(function () {
    game = new Game();
  })

  it('should have properties of name, roundScore and totalScore', function() {
    let names = ['tim', 'taylor', 'bill'];
    game.createPlayers(names);
    expect(game.players[0].name).to.equal('tim');
    expect(game.players[0].roundScore).to.equal(0);
    expect(game.players[0].totalScore).to.equal(0);
  })
})