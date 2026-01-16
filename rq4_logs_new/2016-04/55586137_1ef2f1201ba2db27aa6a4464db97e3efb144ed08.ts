import {Component, Inject} from 'angular2/core';
import {ToDoActions} from './toDoActions';

@Component({
    selector: 'add-todo',
    template:`
        <input #toDoInput>
        <button (click)="addToDo(toDoInput.value)">Add todo</button>
    `
})
export class AddToDo {

    constructor(@Inject('AppStore') private appStore: AppStore,
                private todoActions: ToDoActions) {
    }

    addToDo(text: string) {
        let action = this.todoActions.addToDo(text);
        this.appStore.dispatch(action);
    }
}