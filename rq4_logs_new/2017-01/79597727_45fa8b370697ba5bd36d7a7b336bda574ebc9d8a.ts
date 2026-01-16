import { Component, Input, Output, EventEmitter } from '@angular/core';
import { Meal } from './meal.model';

@Component({
  selector: 'edit-meal',
  template: `
  <div *ngIf="mealToEdit">
    <div class="col-md-3">
      <h3>Add a New Meal:</h3>
      <div class="form-group">
        <label>Enter Date:</label>
        <input [(ngModel)]="mealToEdit.date" class='form-control' type='date'>
      </div>
      <label>Enter Meal Time</label>
      <select [(ngModel)]="mealToEdit.mealTime" class="form-control">
        <option value="Breakfast" >Breakfast</option>
        <option value="Lunch">Lunch</option>
        <option value="Dinner">Dinner</option>
        <option value="Snack">Snack</option>
      </select><br>
      <div class="form-group">
        <label>Enter Meal Details</label><br>
        <input [(ngModel)]="mealToEdit.details" class="form-control">
      </div>
      <button class="btn btn-primary btn-inverse" (click)="finsihButtonClicked()">Finish</button>
    </div>
  </div>
  `
})

export class EditMealComponent{
  @Input() mealToEdit;
  @Output() emittEdit = new EventEmitter()

  finsihButtonClicked() {

    this.emittEdit.emit();
  }

}