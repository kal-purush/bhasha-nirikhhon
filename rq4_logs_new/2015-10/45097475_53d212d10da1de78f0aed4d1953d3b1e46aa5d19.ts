import {Component, View, FORM_DIRECTIVES} from 'angular2/angular2'
import {TaskModel, TaskService} from './task-service'

@Component({
  selector: 'task-input'
})

@View({
  directives: [FORM_DIRECTIVES],
  template: `
    <form (ng-submit)="submitTask()">
      <input type="text" [(ng-model)]="description" />
    </form>
  `
})

export class TaskInput {

  description: string
  taskService: TaskService

  constructor(taskService: TaskService) {
    this.taskService = taskService
  }

  submitTask() {
    const task = new TaskModel(this.description)
    this.taskService.createTask(task)
  }

}