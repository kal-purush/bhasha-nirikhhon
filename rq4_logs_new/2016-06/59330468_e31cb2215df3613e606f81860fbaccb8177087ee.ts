import {Component} from '@angular/core';
import {NavController, NavParams} from 'ionic-angular';
import {Player} from '../../providers/classes/player';
import {Chem} from '../../providers/classes/chem';
import {Settlement} from '../../providers/classes/settlement';
import {SqlService} from '../../providers/services/sql-storage-service';

@Component({
  templateUrl: 'build/pages/travel-dispatcher/travel-dispatcher.html',
  providers: [SqlService]
})
export class TravelDispatcherPage {
  private nav: NavController;
  private navParams: NavParams;
  private sqlService: SqlService;
  private player: Player;
  private destination: Settlement;
  private numRaiders: number;
  private raiderMoney: number;
  private escapeChance: number;
  private playerHitChance: number;
  private raiderHitChance: number;
  private raiderDamage: number;
  private playerEscaped: boolean;
  private playerResultMessage: string;
  private statusMessage: string;
  private raiderResultMessage: string;

  constructor(nav: NavController, navParams: NavParams, sqlService: SqlService) {
    this.nav = nav;
    this.navParams = navParams;
    this.sqlService = sqlService;
    this.player = navParams.get('player');
    this.destination = navParams.get('destination');
    this.numRaiders = Math.floor(Math.random() * 10 + 1);
    this.raiderMoney = Math.floor(Math.random() * 5000 + 1000) * this.numRaiders;
    this.escapeChance = 0.75 - (this.numRaiders * 0.05);
    this.playerHitChance = 0.75;
    this.raiderHitChance = 0.25;
    this.raiderDamage = 10;
    this.playerEscaped = false;
    this.playerResultMessage = '';
    this.statusMessage = this.numRaiders + 
      ' raiders have ambushed you! \nWhat do you do?';
    this.raiderResultMessage = '';
  }

  run() {
    let chance = Math.random();
    if (chance < this.escapeChance) {
      this.playerEscaped = true;
      this.statusMessage = 'You escaped!'
    } else {
      this.playerResultMessage = 'You try to run but can\'t escape';
      this.raidersTakeTurn();
    }
  }

  shoot() {
    let chance = Math.random();
    if (chance < this.playerHitChance) {
      this.playerResultMessage = 'You killed a raider!';
      this.numRaiders -= 1;
      if (this.numRaiders == 0) {
        this.statusMessage = 'All raiders are dead! \n You find ' +
          this.raiderMoney + ' caps on them.';
        this.player.caps += Number(this.raiderMoney);
        return;
      }
    } else {
      this.playerResultMessage = 'You failed to hit.';
    }
    this.raidersTakeTurn();
  }

  raidersTakeTurn() {
    for (let i = 0; i < this.numRaiders; i += 1) {
      let chance = Math.random();
      if (chance < this.raiderHitChance) {
        this.raiderResultMessage = 'You\'ve been hit!';
        this.player.health -= this.raiderDamage;
        break;
      } else {
        this.raiderResultMessage = 'Miss!';
      }
    }
  }
}