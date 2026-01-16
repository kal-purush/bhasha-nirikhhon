import 'todomvc-common';
import 'todomvc-common/base.css';
import 'todomvc-app-css/index.css';

import $ from 'jquery';
import AppView from './views/app-view';
import Backbone from 'backbone';
import TodoRouter from './routers/router';

new TodoRouter();
Backbone.history.start();

$(function() {
  new AppView();
});