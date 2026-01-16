import { Component, EventEmitter } from 'angular2/core';
import { Restaurant } from './restaurant.model';

@Component({
  selector: 'specialties',
  outputs: ['onSpecialtySelect'],
  template: `
    <select (change)="onChange($event.target.value)">
      <option value="Sandwiches" selected="selected">Sandwiches</option>
      <option value="Mexican">Mexican</option>
      <option value="Vietnamese">Vietnamese</option>
      <option value="Italian">Italian</option>
      <option value="Ramen">Ramen</option>
      <option value="New American">New American</option>
      <option value="Other">Other</option>
    </select>
  `
})
export class SpecialtiesComponent {
  public restaurant: Restaurant;
  public onSpecialtySelect: EventEmitter<string>;
  constructor() {
    this.onSpecialtySelect = new EventEmitter();
  }
  onChange(specialtyOption) {
    console.log("child", specialtyOption);
    this.onSpecialtySelect.emit(specialtyOption);
  }
}