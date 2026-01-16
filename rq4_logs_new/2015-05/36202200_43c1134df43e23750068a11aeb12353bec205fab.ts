/// <reference path="../typings/angular2/angular2.d.ts" />
import {Component, View, bootstrap} from 'angular2/angular2';

import {SearchForm} from './components/SearchForm';
import {MigrationMap} from './components/MigrationMap';
import {TodoStore} from './services/TodoStore';

@Component({
  selector: 'migration-app',
  appInjector: [TodoStore]
})
@View({
  templateUrl: './templates/migration-app.html',
  directives: [SearchForm, MigrationMap]
})
class MigrationApp {
  constructor() {
  }
}

bootstrap(MigrationApp);