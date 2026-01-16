/// <reference path="../../../typings/references.d.ts"/>

import {alt} from "../alt";
import Alt = require("alt");
import {Todo} from "../stores/TodoStore";

interface Actions {
  create(text:string);
  updateText(todo:Todo);
  toggleComplete(id:string);
  toggleCompleteAll();
  destroy(id:string);
  destroyCompleted();
}

class TodoActions extends Alt {
  constructor(config) {
    this.generateActions(
      "create",
      "updateText",
      "toggleComplete",
      "toggleCompleteAll",
      "destroy",
      "destroyCompleted");
    super(config);
  }
}

export const todoActions = alt.createActions<Actions>(TodoActions);

export default todoActions;