import {IonicApp, Page, NavController, Events, MenuController} from 'ionic-angular';
import {HomePage} from '../home/home';

import {HttpService} from '../../providers/http-service';

@Page({
  providers: [HttpService],
  templateUrl: 'build/pages/invitation/invitation.html'
})
export class InvitationPage {

  waitingInvitations: any;
  oldInvitations: any;
  stats: Object;
  loading = false;

  constructor(public nav: NavController, public events: Events, public http: HttpService, public menu: MenuController, app: IonicApp) {
    this.nav = nav;
    this.events = events;
    this.http = http;
    this.stats = { "received": 0, "futur": 0, "sent": 0};
    this.loading = true;

    http.makeBackendRequest('GET', 'me/stats', null, response => {
        this.stats = response;
        this.loading = false;
    }, errorMessage => {
      this.loading = false;
        HttpService.showAlert(this.nav, "Error code : " + errorMessage.status, "Notre serveur n'a pas répondu, veuillez réessayez", "Ok");
    }, true);
  }


}