import { Injectable } from '@angular/core';
import { Chart } from '../model/chart';
import { Event } from '../model/event';
import { Lane } from '../model/lane';
import { Stage } from '../model/stage';
import { Task } from '../model/task';
import { Moment } from 'moment';

@Injectable({
  providedIn: 'root'
})
export class ChartStoreService { // are you ready

  chart: Chart;

  constructor() {}

  getChart(): Chart {
    return this.chart;
  }

  addTask(newTask: Task): void {
    this.chart.tasks.push(newTask);
  }

  addLane(newLane: Lane): void {
    this.chart.lanes.push(newLane);
  }

  addEvent(newEvent: Event): void {
    this.chart.events.push(newEvent);
  }

  addStage(newStage: Stage): void {
    this.chart.stages.push(newStage);
  }
}