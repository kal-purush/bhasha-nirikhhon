import {Page, NavController, NavParams} from 'ionic-angular';
import {Player} from '../../providers/classes/player';
import {SqlService} from '../../providers/services/sql-storage-service';

@Page({
  templateUrl: 'build/pages/caravan-hq/caravan-hq.html',
  providers: [SqlService]
})
export class CaravanHqPage {
  private nav: NavController;
  private navParams: NavParams;
  private sqlService: SqlService;
  private player: Player;

  constructor(nav: NavController, navParams: NavParams, sqlService: SqlService) {
    this.nav = nav;
    this.navParams = navParams;
    this.sqlService = sqlService;
    this.player = navParams.get('player');
  }
}