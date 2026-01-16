import { Component, HostListener, OnInit } from '@angular/core';
import { Title } from '@angular/platform-browser';
import { Router, ActivatedRoute } from '@angular/router';

import { Session } from '../../model';
import { ScheduleService } from '../../shared';

@Component({
  selector: 't3dd16-schedule-detail',
  templateUrl: 'schedule-detail.component.html',
  styleUrls: ['schedule-detail.component.scss']
})
export class ScheduleDetailComponent implements OnInit {

  session: Session;

  constructor(private scheduleService: ScheduleService, private router: Router, protected title: Title, private route: ActivatedRoute) {
  }

  ngOnInit(): void {
    this.scheduleService.getByPath(this.route.snapshot.params['session']).subscribe((session) => {
      this.title.setTitle(session.title + ' - ' + this.title.getTitle());
      this.session = session;
    });
  }

  close() {
    this.title.setTitle(this.title.getTitle().substring(this.session.title.length + 3));
    this.router.navigateByUrl('/schedule');
  }

  @HostListener('window:keydown', ['$event']) onKeydown(event: KeyboardEvent) {
    if (event.keyCode === 27) {
      this.close();
    }
  }

}