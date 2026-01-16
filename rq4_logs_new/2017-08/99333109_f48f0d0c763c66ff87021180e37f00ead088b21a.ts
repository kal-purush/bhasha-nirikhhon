import { Component, OnDestroy } from '@angular/core';
import { Subscription } from 'rxjs/Rx';

import { environment } from '../../environments/environment';

import { Gravector } from '../stellarbodies/utils/gravector';
import { StellarBody } from '../stellarbodies/utils/stellarbody';
import { StellarBodyService } from '../stellarbodies/utils/stellarbody.service';

@Component({
  selector: 'app-visualization',
  templateUrl: './visualization.component.html',
  styleUrls: ['./visualization.component.scss']
})
export class VisualizationComponent implements OnDestroy {

  public title = 'Stutterwarp Calculator';
  public start: StellarBody;
  public stop: StellarBody;
  private startSubscription: Subscription;
  private stopSubscription: Subscription;
  public gravs: number;
  public gravectors: Gravector[];
  public distance: number;
  private distanceStart: number;
  private distanceStop: number;

  constructor(private stellarBodyService: StellarBodyService) {
    this.startSubscription = this.stellarBodyService.getStart().subscribe(obj => {
      this.start = obj;
      if (this.stop !== undefined) {
        this.setDistance();
        this.setGravector(obj);
     }
    });
    this.stopSubscription = this.stellarBodyService.getStop().subscribe(obj => {
      this.stop = obj;
      if (this.start !== undefined) {
        this.setDistance();
        this.setGravector(obj);
      }
    });
  }

  public ngOnDestroy(): void {
    this.startSubscription.unsubscribe();
    this.startSubscription.unsubscribe();
  }

  private async setGravector(stellarBody: StellarBody) {
    this.gravectors = [];
    let distance = stellarBody.radius;
    while (stellarBody.id !== 1) {
      const tempDistance = stellarBody.parentDistance;
      stellarBody = await this.calcGravector(stellarBody, distance);
      distance = tempDistance;
    }
    await this.calcGravector(stellarBody, distance);
    this.gravs = this.gravectors.sort((a , b) => (a.grav < b.grav) ? 1 : -1)[0].grav;
  }

  private calcGravector(stellarBody: StellarBody, distance: number): Promise<StellarBody> {
      this.gravectors.push(new Gravector(stellarBody.id, stellarBody.radius, environment.conG * stellarBody.mass / distance ** 2));
      return this.stellarBodyService.getStellarBody(stellarBody.parent);
  }

  public async setDistance() {
     await this.getDistanceStart();
     await this.getDistanceStop();
     this.distance = Math.abs(this.distanceStart - this.distanceStop);
  }

  private async getDistanceStart() {
    this.distanceStart = 0;
    let stellarBody = this.start;
    while (stellarBody.id !== 1) {
      this.distanceStart += stellarBody.parentDistance;
      stellarBody = await this.stellarBodyService.getStellarBody(stellarBody.parent);
      console.log(stellarBody);
    }
  }

  private async getDistanceStop() {
    this.distanceStop = 0;
    let stellarBody = this.stop;
    while (stellarBody.id !== 1) {
      this.distanceStop += stellarBody.parentDistance;
      stellarBody = await this.stellarBodyService.getStellarBody(stellarBody.parent);
    }
  }
}