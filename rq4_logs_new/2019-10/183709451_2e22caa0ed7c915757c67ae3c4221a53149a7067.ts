import { Component, OnInit } from '@angular/core';

import {PlayerService} from '../../services/player.service';

import {PlayerModel} from '../../models/player.model';

@Component({
  selector: 'app-players',
  templateUrl: './players.component.html',
  styleUrls: ['./players.component.css']
})
export class PlayersComponent implements OnInit {

  players: PlayerModel[];

  constructor(
    private playerService: PlayerService
  ) { }

  ngOnInit() {
    this.getPlayers();
  }

  getPlayers() {
    this.playerService.getAllPlayers().subscribe(
      data => {
        this.players = data;
        console.log(this.players);
      }
    );
  }

}