import {Page, NavController, NavParams, Modal, ViewController} from 'ionic-angular';
import {Player} from '../../providers/classes/player';
import {Chem} from '../../providers/classes/chem';
import {Settlement} from '../../providers/classes/settlement';
import {SettlementService} from '../../providers/services/settlement-service';
import {ChemService} from '../../providers/services/chem-service';

@Page({
  templateUrl: 'build/pages/travel-dispatcher/travel-dispatcher.html',
  providers: [ChemService]
})
export class TravelDispatcherPage {
  private nav: NavController;
  private navParams: NavParams;
  private chemService: ChemService;
  private player: Player;
  private destination: Settlement;
  private messages: string[];
  private message: string;

  constructor(nav: NavController, navParams: NavParams, chemService: ChemService) {
    this.nav = nav;
    this.navParams = navParams;
    this.chemService = chemService;
    this.player = navParams.get('player');
    this.destination = navParams.get('destination');
    this.messages = ["funny message 0", "funny message 1", "funny message 2"];
    let index = Math.floor(Math.random() * this.messages.length);
    this.message = this.messages[index];
  }
}