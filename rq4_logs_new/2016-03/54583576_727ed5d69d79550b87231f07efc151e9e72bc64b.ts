import { Component, EventEmitter } from 'angular2/core';
// import { KegComponent } from './keg.component';
import { Keg } from './keg.model';
// import { EditTaskDetailsComponent } from './edit-keg-details.component';
// import { NewTaskComponent } from './new-task.component';
// import {DonePipe} from './done.pipe';

@Component({
  selector: 'keg-list',
  inputs: ['kegList'],
  // outputs: ['onKegSelect'],
  // pipes: [DonePipe],
  // directives: [TaskComponent, EditTaskDetailsComponent, NewTaskComponent],
  template:
    `
    <ul>
      <li *ngFor="#currentKeg of kegList">{{ currentKeg.name }}, ABV {{ currentKeg.ABV }}</li>
    </ul>
    `
  // `
  // <select (change)="onChange($event.target.value)" class="filter">
  //   <option value="all">Show All</option>
  //   <option value="done">Show Done</option>
  //   <option value="notDone" selected="selected">Show Not Done</option>
  // </select>
  // <task-display *ngFor="#currentTask of taskList | done:filterDone"
  //   (click)="taskClicked(currentTask)"
  //   [class.selected]="currentTask === selectedTask"
  //   [task]="currentTask">
  // </task-display>
  // <edit-task-details *ngIf="selectedTask" [task]="selectedTask">
  // </edit-task-details>
  // <new-task (onSubmitNewTask)="createTask($event)"></new-task>
  // `
})
export class KegListComponent {
  public kegList: Keg[];
  // public onKegSelect: EventEmitter<Keg>;
  public selectedKeg: Keg;
  // public filterDone: string = "notDone";
  constructor() {
    // this.onKegSelect = new EventEmitter();
  }
  kegClicked(clickedKeg: Keg): void {
    this.selectedKeg = clickedKeg;
    // this.onKegSelect.emit(clickedKeg);
  }
  // createKeg(newKeg: Keg): void {
  //   this.kegList.push(
  //     new Keg(newKeg.description, this.kegList.length, newKeg.priority, newKeg.category)
  //   );
  //   console.log(this.kegList[this.kegList.length-1]);
  // }
  // onChange(filterOption) {
  //   this.filterDone = filterOption;
  // }
}