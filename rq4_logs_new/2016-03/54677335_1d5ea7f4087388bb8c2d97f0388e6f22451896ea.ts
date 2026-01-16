import { Component, EventEmitter } from 'angular2/core';
import { CdListComponent         } from './cd-list.component';
import { CD                      } from './cd.model';

@Component({
  selector: 'my-app',
  directives: [ CdListComponent ],
  template: `
  <div class="container">
  <h1>Music Store</h1>
    <div class="cd">
      <cd-list [cdList]="cds" (onCdSelect)="cdWasSelected($event)">
      </cd-list>
    </div>
  </div>
  `
})
export class AppComponent {
  public cds: CD[];

  constructor(){
    this.cds = [
      new CD("CD 1", 'Artist 1', 'Genre 1', 9, 0),
      new CD("CD 2", 'Artist 2', 'Genre 2', 12, 1),
      new CD("CD 3", 'Artist 3', 'Genre 3', 15, 2),
      new CD("CD 4", 'Artist 4', 'Genre 4', 22, 3)
    ];
  }
  cdWasSelected(clickedCD: CD): void {
    console.log('Parent', clickedCD);
  }
}