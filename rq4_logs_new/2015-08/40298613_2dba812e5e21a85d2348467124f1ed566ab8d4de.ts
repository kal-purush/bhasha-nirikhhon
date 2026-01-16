
import {Event, Action, createEvent} from "../../../lib/h5flux.d";

import {TodoListData, TodoItemData} from "../data/todo";
import {todolist_was_changed} from "../events/todo";

export var addTodo: Action<TodoListData, string> = {
    name: "ADD_TODO",
    // persist: function(state: TodoListData, text: string) {
    //   return 0;
    // },
    reduce: function(state: TodoListData, text: string) {
        var item: TodoItemData = {
            id: 0,
            marked: false,
            text: text
        };
        return [item].concat(state);
    },
    notify: [
        todolist_was_changed
    ]
}