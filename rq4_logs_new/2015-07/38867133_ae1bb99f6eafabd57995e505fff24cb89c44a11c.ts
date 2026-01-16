/// <reference path="/typings/angular2/angular2.d.ts" />
import {Component, View, bootstrap, NgFor, NgIf, formDirectives} from 'angular2/angular2';
@Component({
  selector: 'todo-list'
})
@View({
  template: `
    <ul>
      <li *ng-for="#todo of todos; #i = index">
        {{ todo }} <button (click)="removeTodo(i)">X</button>
      </li>
    </ul>
    
    <input type='text' placeholder="Enter an item" [(ng-model)]="todotext" (keyup)="doneTyping($event)">
    <button (click)="addTodo()">Add Todo</button>

    <p>You have a total of {{todos.length}} items to do!</p>
    <p *ng-if="todos.length > 3">Get to work you have a lot to do!</p>
    <p *ng-if="todos.length == 0">Looks like you can take it easy.</p>
          `,
  directives: [NgFor, NgIf, formDirectives]
})
class TodoList {
  todos: Array<string>;
  todotext: string;
  
  constructor() {
    this.todos = ["Eat Breakfast", "Walk Dog", "Breathe"];
    this.todotext = '';
  }
  
  addTodo() {
  	if(this.todotext !== '') {
    	this.todos.push(this.todotext);
    	this.todotext = '';
	}
  }

  removeTodo(index: number) {
  	this.todos.splice(index,1);
  }
  
  doneTyping($event) {
    if($event.which === 13) { //13 is carriage return
    	this.addTodo();
    }
  }
}
bootstrap(TodoList);