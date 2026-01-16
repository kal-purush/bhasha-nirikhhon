import {Component} from '@angular/core';
import {NavParams, ViewController} from "ionic-angular";
import {Team} from "../../model/Team";

/**
 * Generated class for the FilterModalComponent component.
 *
 * See https://angular.io/api/core/Component for more info on Angular
 * Components.
 */
@Component({
  selector: 'filter-modal',
  templateUrl: 'filter-modal.html'
})
export class FilterModalComponent {

  teams: Team[];
  activeTeam: Team;
  start: Date;
  end: Date;

  constructor(public viewCtrl: ViewController,
              params: NavParams) {
    this.teams = params.data.teams;
    this.activeTeam = params.data.activeTeam;
    this.start = params.data.start;
    this.end = params.data.end;
  }

  dismiss() {
    const data = {
      teams: this.teams,
      activeTeam: this.activeTeam,
      start: this.start,
      end: this.end
    };

    this.viewCtrl.dismiss(data);
  }
}