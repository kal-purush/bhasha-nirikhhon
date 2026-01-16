import { Component, OnInit } from '@angular/core';
import {Task} from "../../../../../model/task";
import {TaskService} from "../../../../../services/taskService";
import {TaskStatus} from "../../../../../model/taskStatus";

@Component({
    selector: 'approval-list',
    templateUrl: 'approvalList.component.html',
    styleUrls: ['approvalList.component.css']
})

export class ApprovalList implements OnInit {

    tasks: Task[] = [];

    constructor(private taskService: TaskService) {
    }

    ngOnInit() {
        this.loadAllCreatorPendingTasks();
    }

    private loadAllCreatorPendingTasks() {
        this.taskService.getAllCreatorTasksByStatus(TaskStatus.PENDING).subscribe(tasks => { this.tasks = tasks; });
    }

    approve(id : number) {

        this.taskService.changeStatus(id, TaskStatus[TaskStatus.SOLVED]).subscribe(() => {this.loadAllCreatorPendingTasks();});
    }

    reject(id : number) {

        this.taskService.changeStatus(id, TaskStatus[TaskStatus.OPEN]).subscribe(() => {this.loadAllCreatorPendingTasks();});
    }
}