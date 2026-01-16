import {Component} from '@angular/core';
import {UnlessDirective} from './unless.directive';



@Component({
  selector: 'my-structural-directive',
  template: `
    <section class="directive">
      <h2>*ngIf</h2>
        <div>
          Enter a number higher than 10
          <br>
          <input type="text" #number (keyup)="0">
        </div>
        
        <div *ngIf="number.value > 10">
          Number is greater than 10
        </div>  
    </section>
<br>
    <section class="directive-two">
      <h2>*ngFor</h2>
        <div>
          <ul>
            <li *ngFor="let item of list">
              {{item}}
            </li>
          </ul>
        </div>
    </section>
<br>
    <section class="directive-three">
      <h2>[ngSwitch]</h2>
        <div>
          Enter red, blue or green
          <br>
          <input type="text" #color (keyup)="0">
        </div>

        <div [ngSwitch]="color.value">

          <template [ngSwitchWhen]="'red'"><span style="color: red">Color is RED</span></template>
          <template [ngSwitchWhen]="'blue'"><span style="color: blue">Color is blue</span></template>
          <template [ngSwitchWhen]="'green'"><span style="color: green">Color is GReeN</span></template>
          <template ngSwitchDefault><span>unknown color o_O</span></template>

        </div>
    </section>
<br>
    <section class="directive">
      <h2>Custom Directive: *myUnless</h2>
        <div>
          Enter True or False
          <br>
          <input type="text" #condition (keyup)="0">
        </div>
        <div *myUnless="condition.value != 'false'">
          ONLY show if 'false' was entered
        </div>  
    </section>    
  `,
  directives: [UnlessDirective]
})


export class StructuralDirectiveComponent {

  list = ["Abel", "Nikki", "Maiu", "Alexandrov", "Kobe"];

}