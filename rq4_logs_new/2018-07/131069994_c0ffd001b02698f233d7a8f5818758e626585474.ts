import { Component } from '@angular/core';
import {AlertController, LoadingController, NavParams, ViewController} from 'ionic-angular';
import {Bar} from "../../../../_models/bar";
import {MatchesService} from "../../../../_services/matches";
import {EventsService} from "../../../../_services/events";
import {Owner} from "../../../../_models/owner";

@Component({
  templateUrl: 'create-event-modal.html'
})
export class CreateEventModal {

  constructor(
    public params: NavParams,
    public viewCtrl: ViewController,
    private alertCtrl: AlertController,
    private matchesService: MatchesService,
    private loadingController: LoadingController,
    private eventService: EventsService
  ) {}

  createEventCallback: Function;
  matches = [];
  bar:Bar;
  owner:Owner;

  ngOnInit(): void {
    const owner:Owner = this.params.get('owner');
    this.bar = owner.bar;
    this.owner = owner;
    this.fetchMatches();
    this.createEventCallback = this.addEvent.bind(this);
  }

  loading () {
    let loading = this.loadingController.create({
      content: 'Por favor, aguarde...',
      spinner: 'bubbles'
    });

    loading.present();

    return loading;
  }

  dismiss() {
    this.viewCtrl.dismiss();
  }

  _showCreateEventConfirmationAlert (successCb, rejectCb) {
    let alert = this.alertCtrl.create({
      title: "Atenção",
      subTitle: 'Tem certeza que deseja criar o evento?',
      buttons: [
        {
          text: 'Cancelar',
          role: 'cancel',
          handler: rejectCb
        },
        {
          text: 'Sim',
          handler: successCb
        }
      ]
    });
    alert.present();
  }

  addEvent (match) {
    const successCb = () => {
      console.log("eventService.create");
      this.eventService.create(match._id, this.bar._id, this.owner._id).toPromise();
    };
    console.log(match);
    console.log("add event");
    // TODO: Dar bind dessa função com o this desse component
    this._showCreateEventConfirmationAlert(successCb, this.dismiss);
  }

  async fetchMatches () {
    const loading = this.loading();
    const matches = await this.matchesService.getAll().toPromise();
    this.matches = matches.map((match) => {
      const currDate = new Date();
      const date = new Date(match.matchDate);

      date.setTime(date.getTime() + date.getTimezoneOffset()*60*1000);
      match.matchDate = date;

      let timeDiff = currDate.getTime() - date.getTime();
      if(timeDiff/3600000 <= 2.5) {
        return match;
      }
    });
    loading.dismiss();
  }

}