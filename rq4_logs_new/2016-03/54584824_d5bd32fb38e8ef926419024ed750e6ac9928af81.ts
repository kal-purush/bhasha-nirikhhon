import {Component, EventEmitter} from 'angular2/core';

@Component({
  selector: 'new-keg',
  outputs: ['on SubmitNewKeg'],
  template: `
  <div class="keg-form">
  <h3>Add new keg:</h3>
  <input placeholder="Brewer Name" class="col-sm-8 input-lg" #newBrewer>
  <input placeholder="Beer Name" class="col-sm-8 input-lg" #newBeer>
  <input placeholder="Beer Type" class="col-sm-8 input-lg" #newType>
  <button (click)="addKeg(newBrewer, newBeer, newType)" class="btn-success add-button">Add</button>
  </div>
  `
})

export class NewKegComponent {
  public onAddNewKeg: EventEmitter<string>;
    constructor(){
      this.onAddNewKeg = new EventEmitter();
    }
    addKeg()
}