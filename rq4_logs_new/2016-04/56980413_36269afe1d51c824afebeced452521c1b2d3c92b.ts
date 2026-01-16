import {Component} from 'angular2/core';

// annotations come from TS
@Component({
  selector: 'hello',
//  templateUrl: './templates/hello.html',
  template: `
<fieldset>
  <h2>Hello {{name}}, from inline template.</h2><br/>
  <input [(ngModel)]="name"  type="text"/>
  <button type="button" (click)="clickHandler()">Click Me!</button>
</fieldset>`
})

// classes come from es6
export class HelloComponent {
  public name:string = "<your name>";

  constructor() {
  }

  clickHandler(event) {
    alert('Bingo!');
  }
}
