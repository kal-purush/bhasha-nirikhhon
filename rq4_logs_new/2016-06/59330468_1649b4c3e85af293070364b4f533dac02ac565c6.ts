import {Page, NavController, NavParams, Alert} from 'ionic-angular';
import {Player} from '../../providers/classes/player';
import {SqlService} from '../../providers/services/sql-storage-service';

@Page({
  templateUrl: 'build/pages/doctor/doctor.html',
  providers: [SqlService]
})
export class DoctorPage {
  private nav: NavController;
  private navParams: NavParams;
  private sqlService: SqlService;
  private player: Player;
  private fee: number;

  constructor(nav: NavController, navParams: NavParams, sqlService: SqlService) {
    this.nav = nav;
    this.navParams = navParams;
    this.sqlService = sqlService;
    this.player = navParams.get('player');
    this.fee = (Number(100) - this.player.health) * Number(150);
  }

  heal() {
    if (this.fee > this.player.caps) {
      this.presentNotEnoughCapsAlert();
    } else if (this.player.health == 100) {
      this.presentAlreadyHealthyAlert();
    } else {
      this.player.caps -= Number(this.fee);
      this.player.health = 100;
      this.sqlService.savePlayerState(this.player);
    }
  }

  presentNotEnoughCapsAlert() {
    let alert = Alert.create({
      title: 'Denied',
      subTitle: 'Come back when you have more caps!',
      buttons: ['OK']
    });
    this.nav.present(alert);
  }

  presentAlreadyHealthyAlert() {
    let alert = Alert.create({
      title: 'No Need',
      subTitle: 'You are already healthy!',
      buttons: ['OK']
    });
    this.nav.present(alert);
  }
}