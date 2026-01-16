/// <reference path="../typings/angular2/angular2.d.ts" />
import {Component, View, bootstrap} from 'angular2/angular2';

// Annotation section
@Component({
  selector: 'my-app2'
})

@View({
  template: '<h1>Halo {{ name }}</h1>'
})

// Component controller
class MyAppComponent2 {
  name: string;
  
  constructor() {
    this.name = 'John';
  }
}

bootstrap(MyAppComponent2);