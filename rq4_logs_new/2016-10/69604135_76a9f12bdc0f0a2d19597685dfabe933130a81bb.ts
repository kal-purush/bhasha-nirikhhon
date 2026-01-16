import {Component} from '@angular/core';
import {ActivatedRoute} from '@angular/router';
import {OnInit } from '@angular/core';
import {Event} from "../../shared/model/event";
import {Observable} from "rxjs/Rx";
import {TimelinesService} from "../../shared/model/timelines.service";
import {Timeline} from "../../shared/model/timeline";

@Component({
  selector: 'app-timeline-detail',
  templateUrl: './timeline-detail.component.html',
  styleUrls: ['./timeline-detail.component.scss']
})
export class TimelineDetailComponent implements OnInit {

  timeline: Observable<Timeline>;
  events : Observable<Event[]>;

  constructor(private timelinesService: TimelinesService, private route: ActivatedRoute) { }

  ngOnInit() {
    const id = this.route.snapshot.params['id'];

    this.timeline = this.timelinesService.getTimelineByKey(id);
    this.events = this.timelinesService.getEventsForTimeline(id);
  }

}