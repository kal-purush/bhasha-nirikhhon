import { Component, EventEmitter } from 'angular2/core';
import { Restaurant } from './restaurant.model';

@Component({
  selector: 'edit-restaurant-details',
  inputs: ['restaurant'],
  outputs: ['onEndEdit'],
  template: `
    <div class="restaurant-form">
      <h4>Edit Restaurant Information</h4>
      <input [(ngModel)]="restaurant.name" class="input-lg">
      <input [(ngModel)]="restaurant.specialty" class="input-lg">
      <input [(ngModel)]="restaurant.address" class="input-lg">
      <input [(ngModel)]="restaurant.price" type="text" class="input-lg">
      <button (click)=endEdit() class="btn-success btn-lg add-button">Done Editing</button>
      <button (click)=deleteRestaurant() class="btn-success btn-lg add-button">Delete Restaurant</button>
    </div>
  `
})
export class EditRestaurantDetailsComponent {
  public restaurant: Restaurant;
  public onEndEdit: EventEmitter<Restaurant>;
  constructor() {
    this.onEndEdit = new EventEmitter();
  }
  endEdit() {
    this.onEndEdit.emit(this.restaurant);
  }
  deleteRestaurant() {
    this.restaurant.delete = true;
    this.onEndEdit.emit(this.restaurant);
  }
}