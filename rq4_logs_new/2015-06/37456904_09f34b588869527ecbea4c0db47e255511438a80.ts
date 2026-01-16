/// <reference path="typings/angular2/angular2.d.ts" />

import {Component, View, bootstrap, For} from 'angular2/angular2';

@Component({
    selector: "todo"
})
@View({
    template: `
    <h1>My Todo List</h1>
    <ul>
        <li *for="#todo of todos">
            {{ todo }}
        </li>
    </ul>
    <input #todotext (keyup)="doneTyping($event)"/>
    <button (click)="addTodo(todotext.value)">Add ToDo</button>
    `,
    directives: [For]
})
class TodoList {
    todos:Array<string>;

    constructor() {
        this.todos = ["Learn Angular2", "Learn TypeScript", "Learn node.js", "Learn Docker"];
    }

    addTodo(todo:string) {
        this.todos.push(todo);
    }

    doneTyping($event) {
        if ($event.which === 13) {
            this.addTodo($event.target.value);
            $event.target.value = null;
        }
    }
}

bootstrap(TodoList);