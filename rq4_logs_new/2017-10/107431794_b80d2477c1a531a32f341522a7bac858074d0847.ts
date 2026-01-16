import { Component, OnInit, Input, Output, EventEmitter } from '@angular/core';
import { GameStateService  } from '../game-state.service'

@Component({
  selector: 'app-player',
  templateUrl: './player.component.html',
  styleUrls: ['./player.component.scss']
})
export class PlayerComponent implements OnInit {

  @Input() position: string;
  @Output() playerSelection = new EventEmitter();
  constructor(private _gameState:  GameStateService) { }

  ngOnInit() {
  }

  selection(name, position) {
    if (position === 'gold offense') {
      this._gameState.setGoldOffense(name);
    }
    else if (position === 'gold defense') {
      this._gameState.setGoldDefense(name);
    }
    else if (position === 'black offense') {
      this._gameState.setBlackOffense(name);
    }
    else if (position === 'black defense') {
      this._gameState.setBlackDefense(name);
    }
    else {
      throw 'unexpected position';
    }
  }

  playerName: string;
  
    players = [
      {value: 'p1', viewName: 'Kwang'},
      {value: 'p2', viewName: 'Dan'},
      {value: 'p3', viewName: 'Raihan'},
      {value: 'p4', viewName: 'Kevin'},
      {value: 'p5', viewName: 'Jaq'},
      {value: 'p6', viewName: 'Jason'}
    ];
}