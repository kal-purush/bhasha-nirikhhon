/// <reference path="../../typings/angular2-meteor.d.ts" />
import {Component, View} from 'angular2/core';

@Component({
  selector: 'app'
})
@View({
  templateUrl: 'client/app/app.html'
})
export class AppComponent {
  working: String;

  constructor() {
    this.working = `it's working!`;
  }
}