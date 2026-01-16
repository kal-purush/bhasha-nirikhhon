import { Component, Input, Output, EventEmitter } from '@angular/core';
import { Meal } from './meal.model';

@Component({
  selector: 'add-food',
  template: `
  <div *ngIf="selectedMeal">
    <h3>Add add a food</h3>
    <p>{{selectedMeal.date}}</p>
    <div class="form-group">
      <label>Food Type:</label>
      <input #foodType class="form-control">
    </div>
    <div class="form-group">
      <label>Enter the Calories</label><br>
      <input #foodCalories class="form-control">
    </div>
    <button class="btn btn-primary" (click)="submitForm(mealDate.value, mealTime.value, mealDetails.value)">Create Meal</button>
  </div>
  `
})

export class AddFoodComponent{
  @Input() selectedMeal;
}