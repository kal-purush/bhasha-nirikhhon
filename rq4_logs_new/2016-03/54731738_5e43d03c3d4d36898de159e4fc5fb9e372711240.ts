import { Component, EventEmitter } from 'angular2/core';
import { Meal } from './meal.model';

@Component({
  selector: 'new-keg',
  outputs: ['onSubmitNewMeal'],
  template: `
    <div class="meal-form">
      <h3>Create Meal:</h3>
      <input placeholder="Name" class="col-sm-8 input-lg" #newName>
      <input placeholder="Description" class="col-sm-8 input-lg" #newDescription>
      <input placeholder="Calories" class="col-sm-8 input-lg" #newCalories>
      <button (click)="addMeal(newName, newDescription, newCalories)"
    </div>
  `
})
  export class NewMealComponent {
    public onSubmitNewMeal: EventEmitter<any>;
    constructor() {
      this.onSubmitNewMeal = new EventEmitter();
    }
    addMeal(userName: HTMLInputElement, userDescription: HTMLInputElement, userCalories: HTMLInputElement) {
      var mealArray: Array<any> = [userName.value, userDescription.value, userCalories.value];
      this.onSubmitNewMeal.emit(mealArray);
      userName.value = "";
      userDescription.value = "";
      userCalories.value = "";
    }
  }