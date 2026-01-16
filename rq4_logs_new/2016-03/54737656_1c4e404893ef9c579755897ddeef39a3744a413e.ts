import { Component, EventEmitter } from 'angular2/core';
import { MealListComponent } from './meal-list.component';
import { Meal } from './meal.model';

//trunk view "decorator"//

@Component({
  selector: 'my-app',
  directives: [MealListComponent],
  template: `
  <div class="container">
    <h1>Today's Meals:</h1>
    <meal-list
      [mealList]="meals"
      (onMealSelect)="mealWasSelected($event)">
    </meal-list>
  </div>
  `
})

//trunk controller//

export class AppComponent {
  public meals: Meal[];
  constructor(){
    this.meals = []
  }
  mealWasSelected(clickedMeal: Meal): void {
    console.log("parent", clickedMeal);
  }
}