import {TodoRouter} from '../todo-router/todo-router';
import {bootstrap} from 'angular2/platform/browser'
import {ROUTER_PROVIDERS} from 'angular2/router';
import {provide}           from 'angular2/core';
import {LocationStrategy,
        HashLocationStrategy} from 'angular2/router';

bootstrap(TodoRouter, [ROUTER_PROVIDERS, provide(LocationStrategy, {useClass: HashLocationStrategy})]);