// we import Angular's bootstrap function as we need to start the angular app by
// triggering bootstrap function
import {bootstrap} from 'angular2/platform/browser';

// importing HTTP PROVIDERS form angular http library and passing them globally in our app
import {HTTP_PROVIDERS} from 'angular2/http';

// import the root component as we need to start the app with the root component
import {TodoList} from './todolist/todolist';

// import the service we had created for storing web service configurations
// we pass the service in the bootstrap function as we need to access this service throughout
import {TodoSettings} from './services/todo_settings';

// this is the starting point of our app, we are bootstrapping the angular function, starting
// with the rootcomponent TodoList, also we pass directives and classes we need globally in our app
// as a second argument to the bootstrap function
bootstrap(TodoList, [TodoSettings, HTTP_PROVIDERS]);