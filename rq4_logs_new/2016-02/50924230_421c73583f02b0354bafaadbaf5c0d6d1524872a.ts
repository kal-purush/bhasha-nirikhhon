import {bootstrap} from 'angular2/platform/browser';
import {TodoApp} from './app/todo-app';
import {provide} from 'angular2/core';
import {ROUTER_PROVIDERS, LocationStrategy, HashLocationStrategy} from 'angular2/router';

bootstrap(TodoApp, [ROUTER_PROVIDERS,
          provide(LocationStrategy, {useClass: HashLocationStrategy})]);