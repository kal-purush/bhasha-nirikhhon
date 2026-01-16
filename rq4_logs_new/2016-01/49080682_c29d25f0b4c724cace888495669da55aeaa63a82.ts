import {Component, OnInit} from 'angular2/core';
import {Todo} from "./todo";
import {TodoService} from "./todo.service";
import {TodoComponent} from './todo.component'
import {clearPendingTimers} from "angular2/testing";

@Component({
    selector: 'my-app',
    templateUrl: '/app/templates/app.template.html',
    providers: [TodoService],
    directives: [TodoComponent],
    styles: ['.footer{font-variant: small-caps;font-size: 10;}',],
})
export class AppComponent implements OnInit {

    ngOnInit() {
        this._todoService.getTodos()
            .then(todoList => this.initList(todoList));
    }

    public todoList:Todo[];
    public cpt:number;

    constructor(private _todoService:TodoService) {
    }

    initList(aTodoList:Todo[]) {
        this.todoList = aTodoList
        this.refreshCounter();
    }

    addTodo(){
        this.todoList.push({"text": "", "done": false});
        this.refreshCounter();
    }

    refreshCounter() {
        this.cpt = this.todoList.filter(todo=>!todo.done).length;
    }

}