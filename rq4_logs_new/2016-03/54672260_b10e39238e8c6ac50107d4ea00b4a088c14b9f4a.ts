import { Component, EventEmitter } from 'angular2/core';
import { Restaurant } from './restaurant.model';

@Component({
  selector: 'rate-restaurant',
  inputs:['restaurant'],
  template: `
    <select (change)="onChange($event.target.value)" #newRating>
      <option value="rate" selected>Rate</option>
      <option value="1">1</option>
      <option value="2">2</option>
      <option value="3">3</option>
      <option value="4">4</option>
      <option value="5">5</option>
  `
})

export class RateRestaurantComponent {
  public restaurant: Restaurant;
  onChange(rating) {
    if (rating !== "rate") {
      this.restaurant.addReview(parseInt(rating));
      console.log(this.restaurant.reviews);
    }
    // newRating.value="rate";
  }
}