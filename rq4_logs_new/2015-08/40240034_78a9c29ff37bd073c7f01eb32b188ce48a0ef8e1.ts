import {Component, View, EventEmitter} from 'angular2/angular2';
import {GrandParentCmp} from 'client/grandparent';
import {ParentCmp} from 'client/parent';
import {GrandChildCmp} from 'client/grandchild';

@Component({
  selector: 'child',
  events: ['update']
})
@View({
  template: `
  <div style="background-color: orange; padding: 10px;">
    <h1>Child</h1>

    <div style="text-align: center;">
      <p>{{counter}}</p>
      <button (click)="onClick()" class="btn">Click Me</button>
      <button (^click)="onClick()" class="btn">Click Parent</button>
    </div>


    <grand-child></grand-child>
  </div>
  `,
  directives: [GrandChildCmp]
})
export class ChildCmp {
  counter:number;

  constructor() {
    this.counter = 0;
    this.update = new EventEmitter();
  }

  onClick() {
    this.update.next();
  }
}