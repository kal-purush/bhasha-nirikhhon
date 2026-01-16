/// <reference path="/typings/angular2/angular2.d.ts" />
import {Component, View, bootstrap, NgFor, NgIf} from 'angular2/angular2';

class FriendsService {
  names: Array<string>;
  constructor() {
    this.names = ["Alice", "Aarav", "Martín", "Shannon", "Ariana", "Kai"];
  }
}

// Annotation section
@Component({
  selector: 'my-app'
  appInjector: [FriendsService]
})
@View({
  templateUrl: '/app.tpl'
  directives: [NgFor, NgIf]
})
// Component controller
class MyAppComponent {
  name: string;
  names: Array<string>;
  
  constructor(friendsService: FriendsService) {
    this.name = 'Kathleen';
    this.names = friendsService.names;
  }
}

bootstrap(MyAppComponent);