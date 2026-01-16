import {Component} from 'angular2/core';
import {OrderFoodComponent} from './order-food.component';

@Component({
  selector:'order-app',
  template:'<order-food></order-food>',
  directives:[OrderFoodComponent]
})

export class AppComponent {
}