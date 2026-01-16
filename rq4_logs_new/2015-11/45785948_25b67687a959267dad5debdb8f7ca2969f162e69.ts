import {Component} from 'angular2/angular2';

@Component({
  selector: 'app',
  template: `
    <h1>{{title}}</h1>
  `
})
export class App {
  public title = 'My Angular 2 App';
}