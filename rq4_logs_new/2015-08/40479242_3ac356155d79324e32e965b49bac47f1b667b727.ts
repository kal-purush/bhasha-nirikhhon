import {Component, View, Inject} from 'angular2/angular2';
import {RouteParams, routerDirectives} from 'angular2/router';

@Component({
  selector: 'todos-details'
})
@View({
  templateUrl: 'client/todo-details/todo-details.ng.html',
  directives: [routerDirectives]
})
export class TodoDetails {
  constructor(@Inject(RouteParams) routeParams:RouteParams) {
    this.todoId = routeParams.params.todoId;
    
    Tracker.autorun(zone.bind(() => {
      this.todo = Todos.find(this.todoId).fetch()[0];
    }));
  }
}