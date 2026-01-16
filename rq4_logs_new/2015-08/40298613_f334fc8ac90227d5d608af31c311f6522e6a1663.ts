
import {defineAction,
  delegateActionTo,
  delegateActionToMySelf,
  delegateActionToNone} from "../../../lib/h5flux";

import {TodoListData, TodoItemData} from "../data/todo";
import {todolist_was_changed} from "../events/todo";
import {AddTodo} from "./add_todo";

export var TodoCMD = defineAction({
    name: "CMD",
    delegate: function(state: TodoListData, payload:{cmd: string, val?: string}) {
      if (payload.cmd==='add')
        return delegateActionTo(AddTodo, payload.val);
      if (payload.cmd==='drop-all')
        return delegateActionToMySelf();
      return delegateActionToNone();
    },
    reduce: function(state: TodoListData, payload:{cmd: string, val?: string}) {
      return [];
    },
    notify: [
        todolist_was_changed
    ]
})