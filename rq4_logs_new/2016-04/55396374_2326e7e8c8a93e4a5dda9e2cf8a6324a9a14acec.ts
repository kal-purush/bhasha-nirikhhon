import {Component} from 'angular2/core';
import {FamilyList} from './family-list.component';
import {Configuration} from './configuration';

@Component({
  selector: 'my-app',
  templateUrl: 'app/app.component.html',
  directives: [FamilyList],
  providers: [Configuration]
})

export class AppComponent{

}