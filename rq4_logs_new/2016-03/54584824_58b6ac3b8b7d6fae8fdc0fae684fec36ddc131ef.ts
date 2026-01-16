import {Component} from 'angular2/core';
import {Keg} from './keg.model';

@Component({
  selector: 'edit-keg-details',
  inputs: ['keg'],
  template:`
  <div class="keg-form">
     <h3>Edit Keg: </h3>
     <input [(ngModel)]="keg.brewerName" class="col-sm-8 input-lg keg-form"/>
     <input [(ngModel)]="keg.beerName" class="col-sm-8 input-lg keg-form"/>
     <input [(ngModel)]="keg.beerType" class="col-sm-8 input-lg keg-form"/>
   </div>
  `
})
export class EditKegDetailsComponent {
  public keg: Keg;
}