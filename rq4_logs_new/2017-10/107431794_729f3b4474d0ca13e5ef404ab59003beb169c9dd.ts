import { Component, OnInit, Input, Output, EventEmitter } from '@angular/core';
import { GameStateService  } from '../game-state.service'
import { PlayersListService  } from '../players-list.service'

@Component({
  selector: 'app-player-selection',
  templateUrl: './player-selection.component.html',
  styleUrls: ['./player-selection.component.scss']
})
export class PlayerSelectionComponent implements OnInit {

  @Input() position: string;
  @Output() playerSelection = new EventEmitter();
  constructor(private _gameState:  GameStateService, private _playersList: PlayersListService) { }

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

  getPlayersList() {
    //I don't know why, but the name strings need to be wrapped in an oj=bject. Raw array of strings won't work
    var list = this._playersList.getPlayersList();
    list.sort();
    var ret = [];
    for (var i = 0;  i < list.length;  i++) {
      if (list[i] == '') {
        continue;
      }
      ret.push({name: list[i]})
    }
    return ret;
    
  }
  players = this.getPlayersList();

  playerName: string;
  
    
}