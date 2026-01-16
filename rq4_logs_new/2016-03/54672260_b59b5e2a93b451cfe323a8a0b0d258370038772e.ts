import { Component, EventEmitter } from 'angular2/core';
import { Restaurant } from './restaurant.model';

@Component({
  selector: 'new-restaurant',
  outputs: ['onSubmitNewRestaurant'],
  template: `
    <div class="restaurant-form">
      <h4>Add a Restaurant:</h4>
      <form (submit)="addRestaurant(newName, newSpecialty, newAddress, newPrice)">
        <input required placeholder="name" class="col-sm-8 input-lg" #newName>
        <input required placeholder="specialty" class="col-sm-8 input-lg" #newSpecialty>
        <input required placeholder="address" class="col-sm-8 input-lg" #newAddress>
        <input required placeholder="price" type="text" class="col-sm-8 input-lg" #newPrice>
        <button type="submit" class="btn-success btn-lg add-button">Add</button>
      </form>
    </div>
  `
})

export class NewRestaurantComponent {
  public onSubmitNewRestaurant: EventEmitter<string[]>;
  constructor() {
    this.onSubmitNewRestaurant = new EventEmitter();
  }
  addRestaurant(newName: HTMLInputElement, newSpecialty: HTMLInputElement, newAddress: HTMLInputElement, newPrice: HTMLInputElement){
    var newRestaurantInputs: string[] = [];
    newRestaurantInputs.push(newName.value);
    newRestaurantInputs.push(newSpecialty.value);
    newRestaurantInputs.push(newAddress.value);
    newRestaurantInputs.push(newPrice.value);

    this.onSubmitNewRestaurant.emit(newRestaurantInputs);
    newName.value="";
    newSpecialty.value="";
    newAddress.value="";
    newPrice.value="";
  }
}