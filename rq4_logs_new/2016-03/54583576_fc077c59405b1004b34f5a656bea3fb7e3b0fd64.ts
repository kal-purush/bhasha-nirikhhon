import {Component, EventEmitter} from 'angular2/core';
import {Keg} from './keg.model';


@Component({
  selector: 'edit-keg-details',
  inputs: ['keg'],
  template: `
  <div class="keg-form">
    <h3>Edit {{ keg.name }}</h3>
    <h4>Add Type: </h4>
    <input class="col-sm-8 input-md keg-form" #addType /><br>
    <h4>Edit ABV: </h4>
    <input type="number" step="0.01" class="col-sm-8 input-md keg-form" #editABV /><br>
    <h4>Edit Price: </h4>
    <input type="number" class="col-sm-8 input-md keg-form" #editPrice />
    <button (click)="editKeg(addType, editABV, editPrice)" class="btn-success btn-lg add-button">Edit</button>
  </div>
  `
})

export class EditKegDetailsComponent {
  public keg: Keg;
  public onEditKeg: EventEmitter<Keg>
  constructor(){
    this.onEditKeg = new EventEmitter;
  }
  editKeg(addType: HTMLInputElement, editABV: HTMLInputElement, editPrice:HTMLInputElement){
    console.log(editABV.valueAsNumber);
    this.keg.type.push(addType.value);
    this.keg.ABV = editABV.valueAsNumber;
    this.keg.price = editPrice.valueAsNumber;
    console.log(this.keg);
    this.onEditKeg.emit(this.keg);
  };
}