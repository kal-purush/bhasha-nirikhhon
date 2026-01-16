import {Component, View} from 'angular2/angular2';
import {Todo} from 'Todo';

@Component({
  selector: 'add-todo',
  properties: ['todos']
})
@View({
  template: `
    <div class="panel panel-default">
      <div class="panel-heading">
        <h3 class="panel-title"><span class="glyphicon glyphicon-plus-sign"></span> Add New Todo</h3>
      </div>
      <div class="panel-body">
        <div class="form-group">
          <label for="titleInput">Title:</label>
          <input id="titleInput" class="form-control" placeholder="Title" #title>
        </div>
        <div class="form-group">
          <label for="descriptionInput">Description:</label>
          <input id="descriptionInput" class="form-control" placeholder="Description" #description>
        </div>
        <div class="form-group">
          <label id="responsibleInput">Responsible:</label>
          <input id="responsibleInput" class="form-control" placeholder="Responsible" #responsible>
        </div>
        <div>
          <button class="btn btn-default" (click)="addTodo(title.value, description.value, responsible.value)">Add Todo</button>
        </div>
      </div>
    </div>
  `
})
export class AddTodo {
  todos: Array<Todo>;
  constructor (){
  }
  addTodo(title: string, description: string, responsible: string) {
    this.todos.push(new Todo(title, description, responsible));
  }
}