/// <reference path="/typings/angular2/angular2.d.ts" />
import {Component, View, bootstrap, NgFor} from 'angular2/angular2';

// Annotation section
@Component({
  selector: 'array-demo'
})
@View({
  template: `
  	<h1>Array Demo</h1>
  	<h2>List of Terminators</h2>
    <ul>
      <li *ng-for="#terminator of terminators">
        {{ terminator }}
      </li>
    </ul>
  `,
  directives: [NgFor]
})
// Component controller
class ArrayDemoComponent {
  terminators: Array<string>;
  
  constructor() {
    this.terminators = ['T-800','T-1000','T-X','T-3000'];
  }
}

bootstrap(ArrayDemoComponent);