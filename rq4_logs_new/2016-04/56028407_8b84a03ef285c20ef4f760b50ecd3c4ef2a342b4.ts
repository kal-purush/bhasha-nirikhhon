import {Component} from 'angular2/core';
import {NavigationComponent} from './../navs/navigation';

@Component({
  // templateUrl: 'app/components/board/board.html'
  template: 'foo'
})
export class Board extends NavigationComponent {
  constructor() {
    super('home');
  }
};