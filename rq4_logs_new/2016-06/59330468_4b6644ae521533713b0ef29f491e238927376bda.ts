import {Page, NavController, NavParams} from 'ionic-angular';
import {Player} from '../../providers/classes/player';
import {SqlService} from '../../providers/services/sql-storage-service';

@Page({
  templateUrl: 'build/pages/bank/bank.html',
  providers: [SqlService]
})
export class BankPage {
  private nav: NavController;
  private navParams: NavParams;
  private sqlService: SqlService;
  private player: Player;
  private amount: number;

  constructor(nav: NavController, navParams: NavParams, sqlService: SqlService) {
    this.nav = nav;
    this.navParams = navParams;
    this.sqlService = sqlService;
    this.player = navParams.get('player');
    this.amount = 0;
  }

  deposit(amount: number) {
    if (amount < 0 || amount > this.player.caps) {
      alert("transaction denied, try again");
    } else {
      this.player.caps -= Number(amount);
      this.player.bank += Number(amount);
      this.sqlService.savePlayerState(this.player);
    }
  }

  withdraw(amount: number) {
    if (amount < 0 || amount > this.player.bank) {
      alert("transaction denied, try again");
    } else {
      this.player.caps += Number(amount);
      this.player.bank -= Number(amount);
      this.sqlService.savePlayerState(this.player);
    }
  }
}