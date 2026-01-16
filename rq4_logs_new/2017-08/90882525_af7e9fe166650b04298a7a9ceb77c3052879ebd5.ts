import { Injectable } from '@angular/core';
import { Http, Response } from '@angular/http';

import { Player } from '../model/Player';

import { ApplicationService } from './application.service';

import { Observable } from 'rxjs/observable';
import 'rxjs/add/operator/map';

@Injectable()
export class StatsService {

  playerMaxWins: Player;
  playerMaxRuns: Player;

  constructor(private appService: ApplicationService,
              private http: Http) {
  }

  getMaxWinsPlayer(): void {
    this.http.get(this.appService.baseUrl + '/playerStats/maxWins').map(
      (response: Response) => {
        console.log('getMaxWinsPlayer: ' + response);
        return response.json();
      }
    ).subscribe(
      (player: Player) => this.playerMaxWins = player,
      (error: any) => console.log(error)
    );
  }

  getMaxRunsPlayer(): void {
    this.http.get(this.appService.baseUrl + '/playerStats/maxRuns').map(
      (response: Response) => {
        console.log('getMaxRunsPlayer: ' + response);
        return response.json();
      }
    ).subscribe(
      (player: Player) => this.playerMaxRuns = player,
      (error: any) => console.log(error)
    );
  }

  submitScore(player: Player): any {
    return this.http.post(this.appService.baseUrl + '/playerStats/submit', player).map(
      (response: Response) => {
        console.log('submitScore: ' + response);
        return response.text();
      }
    );
  }

}