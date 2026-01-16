import {Component} from 'angular2/core';
import {Task} from './task';

@Component ({
  selector: 'points',
  inputs: ['task', 'fakeList'],
  template: `
  <div>{{task.points}}</div>
  <button (click) = "morePoints(task)">+1 Point</button>
  <button (click) = "lessPoints(task)">-1 Point</button>
  <button (click) = "deleteTest(task, fakeList)">Delete??</button>
  `
})

export class PointsComponent {
  public task = new Task("hey", 3);
  public fakeList: Task[] = [];


morePoints(clicked: Task): void {
  clicked.points = clicked.points + 1;
  console.log(clicked.points);
}

lessPoints(clicked: Task): void {
  clicked.points = clicked.points - 1;
  console.log(clicked.points);
}

deleteTest(beerSelected: Task, fakeList: Task[]) {
  for (var i =0; i<fakeList.length; i++){
    if(beerSelected.id === fakeList[i].id){
      fakeList.splice(i, 1);
      return true;
    }
  }
}




}