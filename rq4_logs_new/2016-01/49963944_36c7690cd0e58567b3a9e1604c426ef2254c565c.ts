import {Component}   from 'angular2/core';
import {TodoService} from '../todoServcie/todoService.service';
import {OnInit}      from 'angular2/core';

@Component({
  selector    : 'todo-app',
  styleUrls   : ['app/todo/todo.component.css'],
  templateUrl : 'app/todo/todo.component.html',
  providers   : [TodoService]
})
export class TodoComponent implements OnInit {

  public title = 'M.E.A2.N ToDo App';
  public todos;

  ngOnInit() {
    this.getHeroes()
  }

  constructor(private _todoService: TodoService) { }

  getHeroes() {
    this._todoService.getAll()
        .subscribe(todos => this.todos = todos,
                   err   => this.logError(err),
                   ()    => console.log('Operation Complete'));
  }

  addTodo($event, todoText) {

    if($event.which === 13) {
      var _todo = {
        text : todoText.value,
        isCompleted : false
      };

      // calling the service to save the todo
      this._todoService.save(_todo)
          .subscribe(
             data => {
                       this.todos.push(data);
                       todoText.value = '';
                     },
             err  => console.log(err),
             ()   => console.log("data saved"));
    }
  }

  updateTodoText($event, todo){
  	if($event.which === 13){
  		todo.text = $event.target.value;
      var _todo = {
        _id : todo._id,
        text : todo.text ,
        isCompleted : todo.isCompleted
      };

      // calling the service to update the todo text
      this._todoService.update(_todo)
          // wait for the response before resetting the state
          .subscribe(data => this.setEditState(todo, false),
                     err  => console.log(err),
                     ()   => console.log("updated..."));
  	}
  }

  updateStatus(todo){
     var _todo = {
        _id : todo._id,
        text : todo.text ,
        isCompleted : !todo.isCompleted
      };

      // calling the service to update the todo status
      this._todoService.update(_todo)
          // wait for the response before updating the UI
          .subscribe(data => todo.isCompleted = !todo.isCompleted,
                     err  => console.log(err),
                     ()   => console.log("updated..."));

  }

  deleteTodo(todo){
    var todos = this.todos;

    // calling the service to delete the todo
  	this._todoService.delete(todo._id)
        .subscribe(data => {
                     if(data.n == 1){
                        // save a n/w call by updating the local array
                        // instead of making a GET call again to refresh the data
                        for (var i in todos) {
                          if(todos[i]._id == todo._id){
                            todos.splice(i, 1);
                          }
                        }
                      }
                   },
                   err  => console.log(err),
                   ()   => console.log("data deleted"));
  }

  setEditState(todo, state){
  	if(state){
  	  	todo.isEditMode = state;
  	}else{
  		// don't store unwanted presentation logic in DB :/
  		delete todo.isEditMode;
  	}
  }

  logError(err) {
    console.error('There was an error: ' + err);
  }
}