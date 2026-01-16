import { Component, OnInit } from '@angular/core';
import { TodoListService } from "./todo-list.service";
import { Todo } from "./todo";

@Component({
    selector: 'todo-component',
    templateUrl: 'todo.component.html'
})
export class TodoComponent implements OnInit {
    public todo: Todo = null;
    private _id: string;

    constructor(private todoListService: TodoListService) {
        // this.users = this.userListService.getUsers();
    }

    private subscribeToServiceForId() {
        if (this._id) {
            this.todoListService.getUserById(this._id).subscribe(
                todo => this.todo = todo,
                err => {
                    console.log(err);
                }
            );
        }
    }

    setId(_id: string) {
        this._id = _id;
        this.subscribeToServiceForId();
    }

    ngOnInit(): void {
        this.subscribeToServiceForId();
    }
}