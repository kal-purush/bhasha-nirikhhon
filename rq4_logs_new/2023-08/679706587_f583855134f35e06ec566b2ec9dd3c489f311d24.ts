import {Component, OnInit} from '@angular/core';
import {SquadResponse} from "../../types/SquadResponse";
import {SquadRequest} from "../../types/SquadRequest";
import {TaskResponse} from "../../types/TaskResponse";
import {TaskRequest} from "../../types/TaskRequest";
import {TaskService} from "../../services/task.service";

@Component({
  selector: 'app-tasks',
  templateUrl: './tasks.component.html',
  styleUrls: ['./tasks.component.css']
})
export class TasksComponent implements OnInit{
  squads: SquadResponse[] = [];
  squadForAdd: SquadRequest = new SquadRequest();
  tasks: TaskResponse[] = [];
  task: TaskRequest = new TaskRequest();
  currentSquadId: number = -1;
  showTasks: boolean = false;

  constructor(private taskService: TaskService) {
  }

  ngOnInit(): void {
    this.getAllSquadsByUser();
  }

  getAllSquadsByUser() {
    this.taskService.getAllSquadByUser().subscribe({
      next: (squads) => {
        this.squads = squads;
      },
      error: (error) => {
        console.log(error);
      }
    })
  }

  getAllTasksBySquad(squadId: number) {
    this.currentSquadId = squadId;
    console.log(squadId);
    this.taskService.getAllTasksBySquad(squadId).subscribe({
      next: (tasks) => {
        this.tasks = tasks;
        this.showTasks = true;
      },
      error: (error) => {
        console.log(error);
      }
    })
  }

  createNewSquad() {
    this.taskService.createNewSquad(this.squadForAdd).subscribe({
      next: (squad) => {
        this.squads.push(squad);
        this.squadForAdd = new SquadRequest();
      },
      error: (error) => {
        console.log(error);
      }
    })
  }

  deleteSquad(id: number) {
    this.taskService.deleteSquad(id).subscribe({
      next: () => {
        this.getAllSquadsByUser();
      },
      error: (error) => {
        console.log(error);
      }
    })
    if (this.tasks.length < 1) {
      this.showTasks = false;
    }
  }

  addTaskToSquad() {
    this.taskService.addTaskToSquad(this.task, this.currentSquadId).subscribe({
      next: (task) => {
        this.getAllTasksBySquad(this.currentSquadId);
        this.task = new TaskRequest();
      },
      error: (error) => {
        console.log(error);
      }})
  }

  updateTask(task: any) {
    this.taskService.updateTask(task.id, task)
      .subscribe({
        next: (updatedTask) => {
        },
        error: (error) => {
          if (error.status === 403) {
            // this.messageService.showMessage("This is not your task. You cannot update it");
            // } else {
            //   this.messageService.showMessage("Something went wrong");
          }
        }
      })
  }

  deleteTaskFromSquad(taskId: number) {
    this.taskService.deleteTaskFromSquad(this.currentSquadId, taskId).subscribe({
      next: () => {
        this.getAllTasksBySquad(this.currentSquadId);
      },
      error: (error) => {
        console.log(error);
      }})
  }
}