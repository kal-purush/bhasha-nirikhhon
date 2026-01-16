import {Component, OnInit} from 'angular2/core';

import {DaysService} from '../services/days.service';
import {Day} from '../interfaces/day';

@Component({
  selector: 'week-view',
  templateUrl: 'app/components/week-view.html',
  providers: [DaysService]
})
export class WeekViewComponent implements OnInit {
  public days: Day[];

  constructor(private _daysService: DaysService) { }

  getDays() {
    this.days = this._daysService.getDays();
  }

  ngOnInit() {
    this.getDays();
  }
}