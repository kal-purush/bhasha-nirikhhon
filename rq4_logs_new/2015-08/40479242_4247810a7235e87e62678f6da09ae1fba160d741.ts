import {Component, View, NgFor, bootstrap} from 'angular2/angular2';

@Component({
  selector: 'app'
})
@View({
  //template: "<p>Hello World!</p>"
  templateUrl: 'client/index.ng.html',
  directives: [NgFor]
})
class MeteorAngular01 {
  constructor() {
    this.todos = [
      {
        'title': 'Implement data model for ToDo-App',
        'responsible': 'Sebastian',
        'completed': false
      },
      {
        'title': 'Test new functionality',
        'responsible': 'Sebastian',
        'completed': true
      },
      {
        'title': 'Review technical concept',
        'responsible': 'Sebastian',
        'completed': false
      }
    ];
  }
}

bootstrap(MeteorAngular01);