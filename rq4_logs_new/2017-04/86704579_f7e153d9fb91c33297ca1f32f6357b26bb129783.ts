import { bindable } from 'aurelia-framework';

const thing = require('../drawing.svg');

export class NavBar {
  @bindable router: any;

  thing: any = thing;
}