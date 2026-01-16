
import {Component, Inject} from "angular2/core";
import {ToDoActions} from "./toDoActions";

@Component({
    selector: 'todo',
    inputs: ['completed', 'id'],
    template: `
        <li [ngClass]="{'completed': completed}" (click)="completeToDo()">
            <ng-content></ng-content>
        </li>
    `,
})
export class ToDo {
    private completed: boolean;
    private id: number;
    
    constructor(@Inject('AppStore') private appStore: AppStore,
                private todoActions: ToDoActions) {
    }
    
    private completeToDo() {
        console.log('complete:' + this.id);
        this.appStore.dispatch(this.todoActions.completeToDo(this.id));    
    }
}