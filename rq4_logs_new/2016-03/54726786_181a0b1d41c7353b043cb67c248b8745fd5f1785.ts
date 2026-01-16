import { Component, EventEmitter } from 'angular2/core';
import { Meal } from './meal.model';

@Component({
  selector: 'new-meal',
  outputs: ['onSubmitNewMeal'],
  template: `
    <div class="container">
      <h4>Enter a Meal</h4>
        <input placeholder="Name" class="col-sm-8 input-lg" #newName>
        <input placeholder="Details" class="col-sm-8 input-lg" #newDetails>
        <input placeholder="Calories" class="col-sm-8 input-lg" #newCalories>
        <button (click)="addMeal(newName, newDetails, newCalories)">Add this meal</button>
    </div>
  `
})

export class NewMealComponent{
  public onSubmitNewMeal: EventEmitter<any[]>;
  constructor(){
    this.onSubmitNewMeal = new EventEmitter();
  }
  addMeal(newName: HTMLInputElement, newDetails: HTMLInputElement, newCalories: HTMLInputElement) {

    var newMeal = [newName.value, newDetails.value, newCalories.value];

    this.onSubmitNewMeal.emit(newMeal);
    newName.value="";
    newDetails.value="";
    newCalories.value="";
  }
}