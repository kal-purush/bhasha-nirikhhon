/// <reference path="typings/angular2/angular2.d.ts" />
import {Component, View, bootstrap, For, If} from 'angular2/angular2';

class Store {
  todos: Array<{title: String, active: Boolean}>;
  constructor() {
    this.todos = [{title: 'wow', active: true}, {title: 'such', active: false}, {title: 'list', active: true}];
  }
  remove(todo: {title: String, active: Boolean}) {
    for (let todo of this.todos) {
      if (todo.title === todo.title) {
        this.todos.splice(this.todos.indexOf(todo), 1);
        break;
      }
    }
  }
}

@Component({
  selector: 'sample-app',
  injectables: [
    Store
  ]
})
@View({
  directives: [For, If],
  template: `
        <ul class="todo-list">
          <li *for="#todo of store.todos" [class.active]="todo.active">
            <p *if="todo.active" (click)="remove(todo)"> {{todo.title}}</p>
            <p *if="!todo.active" (click)="remove(todo)"> wow not active</p>
          </li>
        </ul>`
})
class SampleApp{
  store: Store;
  constructor() {
    this.store = new Store();
  }
  remove(todo: {title: String, active: Boolean}) {
    todo.active = !todo.active;
    return this.store.remove(todo);
  }
}

bootstrap(SampleApp);