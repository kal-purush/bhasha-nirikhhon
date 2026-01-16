import {Component, View, bootstrap} from 'angular2/angular2';
import {SomeComponent} from 'component';

@Component({
  selector: 'app'
})
@View({
  template: '<some-component></some-component>',
  directives: [SomeComponent]
})
class App{}

bootstrap(App);