import {Component}                 from 'angular2/core';
import {TodoComponent}             from '../todo/todo.component';
import {AppFooter}                 from './app-footer';
import {AppHeader}                 from './app-header';

@Component({
  selector   : 'app-wrapper',
  template   : `
                <app-header></app-header>
                <todo-app></todo-app>
                <app-footer></app-footer>
              `,
  directives: [AppHeader,TodoComponent,AppFooter]
})
export class AppWrapper {}