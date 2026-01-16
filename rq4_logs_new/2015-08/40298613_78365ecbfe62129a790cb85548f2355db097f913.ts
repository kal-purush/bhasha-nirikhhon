import {createH5Event}  from "../../../lib/h5-flux.d";

export var todo_was_edited = createH5Event<Todo_Item>('todo_was_edited');
export var todo_was_saved = createH5Event<Todo_Item>('todo_was_edited');

type Todo_Item = {
    id: number,
    title: string,
    done: boolean
}
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