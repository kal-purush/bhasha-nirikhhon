import { Log } from '../../shared/services/log.service';
import { Component, OnInit, Input } from '@angular/core';

import { ReadService } from '../../shared/services/read.service';
import { OrderbyPipe } from '../../shared/filters';

import { Event } from '../../shared/model/event';

import { Router } from '@angular/router';

/**
* same timing animations
*/
@Component({
  moduleId: module.id,
  selector: 'app-my-calendar-feed',
  templateUrl: 'calendarfeed.component.html',
  styleUrls: ['calendarfeed.component.css']

})

export class CalendarFeedComponent implements OnInit {

  imageservice = '';

  @Input() max = 2;

  items: any[] = [];

  type = 'calendar';

  constructor(private dataService: ReadService,
    private router: Router,
    private log: Log,
    private orderby: OrderbyPipe) {
  }

  ngOnInit(): void {

    this.dataService.getAll(this.type)
                              .subscribe((data: any[]) => this.items = data,
                                  error => this.log.debug(this.type + ' ' + error),
                                  () =>  {

                                    if (this.max > 0 && this.items.length > this.max) {
                                       this.items = this.items.slice(this.items.length - this.max, this.items.length);
                                    }
                                    this.log.debug(this.type + ' '  +  this.items.length);

                                    this.orderby.transform(this.items, 'date', 'desc');

                                });

  }

  getUrl(item): string {
    return 'calendar/detail/' + item.id;
  }


}