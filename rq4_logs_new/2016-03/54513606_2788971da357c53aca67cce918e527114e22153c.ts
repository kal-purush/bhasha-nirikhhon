import {Component} from 'angular2/core';
import {Keg} from './keg.model';

@Component ({
  selector: 'edit-keg-details',
  inputs: ['keg'],
  template: `
  <div class="keg-form">
  <h3>Edit Keg Properties: </h3>
  <input [(ngModel)]="keg.description" class="col-sm-8 input-lg keg-form">
  `
})
export class EditKegDetailsComponent {
  public keg: Keg;
}