import {Component, EventEmitter} from 'angular2/core';
import {Task} from './task';
import {capitalize} from './task';
import {NewTaskComponent} from './new-task.component';
import {DeleteTaskComponent} from './delete-task.component';
import {AppComponent} from './app.component';

@Component ({
  selector: 'edit-task',
  inputs: ['task'],
  template: `
    <div class="task-form">
      <h3>Edit Description: </h3>
      <input [(ngModel)]="task.description" class="col-sm-8 input-lg task-form"/>
    </div>
  `
})
export class EditTaskComponent {
  public task = new Task("hey", 3);
}

// *ngIf = "selectedTask" [task] = "selectedTask">
 // [(ngModel)] = "task.description"