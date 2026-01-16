import {Component} from '@angular/core';
import {AngularFire, FirebaseListObservable} from 'angularfire2';

@Component({
  selector: 'timeline-list',
  templateUrl: './timeline-list.component.html',
  styleUrls: ['./timeline-list.component.scss']
})
export class TimelineListComponent {

  timelines: FirebaseListObservable<any>;

  constructor(af: AngularFire) {

    this.timelines = af.database.list('/timelines', {
      query: {
        limitToLast: 6,
        orderByChild: 'created_date'
      }
    });
  }
}