import {Component} from 'angular2/core';
import {Keg} from './keg.model';
import { EditKegDetailsComponent } from './edit-keg-details.component';

@Component({
  selector: 'show-keg-progress',
  inputs: ['keg'],
  directives: [EditKegDetailsComponent],
  template:`
  <div class="kegProgress">
     <h3>{{ keg.brewerName }}'s {{ keg.beerName }} Progress: {{ (keg.kegLevel/127*100).toPrecision(3) }} % </h3>
     <button (click)="reduceByPint()" class="btn btn-info">Sold a pint?</button>
     <button (click)="reduceByGrowler()" class="btn btn-info">Sold a growler?</button>
   </div>
  `
})
export class ShowKegProgressComponent {
  public keg: Keg;
  reduceByPint(): void {
    this.keg.kegLevel --;
  }
  reducebyGrowler(): void {
    this.keg.kegLevel -=4; ////this does not work. number notation of -= is correct though
  }
}


///http://arjunu.com/2016/01/basic-angular-2-off-click-directive/ USE THIS WEBSITE TO HELP WITH CLICK HIDE/SHOW