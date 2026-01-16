import {Component, EventEmitter} from 'angular2/core';
import {Task} from './task';
import {capitalize} from './task';
import {NewTaskComponent} from './new-task.component';


@Component({
  selector: 'my-app',
  directives: [NewTaskComponent],
  template: `
  <h1>hi</h1>
  <new-task (emitTasks)="concatArrays($event)"></new-task>

  <h3 *ngFor="#task of tasks">{{task.description}}</h3>

  `
})

export class AppComponent {
  public tasks: Task[] = [];
  constructor() {
    this.tasks = [
      new Task("Learn Kung Fu", 0),
      new Task("Learn Kung Fu", 1),
      new Task("Learn Kung Fu", 2),
      new Task("Create To-Do List app.", 3)
    ]
  }
  concatArrays(newTasks: Task[]): void {
    this.tasks = this.tasks.concat(newTasks);
    console.log(this.tasks);
  }


}