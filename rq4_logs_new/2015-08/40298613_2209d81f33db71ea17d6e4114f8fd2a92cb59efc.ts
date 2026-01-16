import {
createFieldString,
createFieldBoolean,
createStore} from "../../../lib/h5-flux.d";

import {TodoItemData} from "../data/todoitem";
import {todoitem_was_edited} from "../events/todoitem";

var instance = {
    finder,
    schema,
    emit: {
        todo_was_edited: todoitem_was_edited.refEmitter()
    },
    listen: {}
};

var schema = {
    title: createFieldString("title", { caption: "Título", hint: "Título da tarefa" }, true),
    done: createFieldBoolean("done", { caption: "Feito", hint: "Tarefa feita" }, true)
};
 
function finder(key: number, callback: (err: Error, data: TodoItemData) => void) {
    if (key)
        callback(null, sample_data[key - 1]);
    else
        callback(null, <TodoItemData>{});
}

export var todoItemStore = createStore(0, instance);

var sample_data: TodoItemData[] = [
    { title: 'task 1', done: false },
    { title: 'task 2', done: true },
    { title: 'task 3', done: false }
];
//
// type H5Type<T> = {
//     toString(value: T): string;
//     fromString(text: string): T;
//     validate(value: T): string[];
// };
//
// type H5Field = {
//   name: string,
//   type: H5Type,
// }
//
// function createH5Schema<T>(name: string, fields)
// {
//
// }
//
// var Todo_Item_Schema : H5Schema = {
//     id: h5number(),
//     title: string,
//     done: boolean
// }
//
//
// var Todo_Item_Store = {
//     state: Todo_Item,
//     events: {
//         emit: {
//             todo_was_edited: todo_was_edited.emitter,
//             todo_was_saved: todo_was_saved.emitter
//         },
//         catches: []
//     }
// }