import { Injectable } from '@angular/core';
import * as moment from 'moment';
import * as d3 from 'd3';

import { Task } from './task';

@Injectable({
  providedIn: 'root'
})
export class D3Service {

  test: Task[];
  roadmap: any;

  constructor() {
    this.test = [
      {
        'id': 0,
        'name': 'default',
        'start': moment('2018-07-01'),
        'end': moment('2018-07-20'),
        'lane': 0
      },
      {
        'id': 0,
        'name': 'default',
        'start': moment('2018-07-05'),
        'end': moment('2018-07-20'),
        'lane': 1
      },
      {
        'id': 0,
        'name': 'default',
        'start': moment('2018-07-21'),
        'end': moment('2018-08-30'),
        'lane': 1
      },
    ];
  }

  init(): void {
    this.roadmap = d3.select('#roadmap')
      .append('svg')
      .style('width', 1000)
      .style('height', 1000)
  }

  findEarliestDate(dates: Task[]): moment.Moment {
    let earliest = moment('2100-01-01');

    dates.forEach(date => {
      if (date.start.isBefore(earliest)) {
        earliest = date.start;
      }
    });

    return earliest;
  }

  findLatestDate(dates: Task[]): moment.Moment {
    let latest = moment('1900-01-01');

    dates.forEach(date => {
      if (date.end.isAfter(latest)) {
        latest = date.end;
      }
    });

    return latest;
  }

  drawTasks(data): void {
    const earliest = this.findEarliestDate(data);

    this.roadmap
      .selectAll('rect')
      .data(data)
      .enter()
      .append('rect')
      .attr('x', (d: any, i) => {
        return moment.duration(d.start.diff(earliest)).asDays();
      })
      .attr('width', (d: any, i) => {
        return moment.duration(d.end.diff(d.start)).asDays();
      })
      .attr('y', (d: any, i) => {
        return 105 * d.lane;
      })
      .attr('height', 100)
  }

  testRectangles(): void {
    this.drawTasks(this.test);
  }
}