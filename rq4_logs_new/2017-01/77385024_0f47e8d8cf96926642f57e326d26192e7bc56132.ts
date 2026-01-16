import { Component, OnInit , EventEmitter, Output} from '@angular/core';
import { Router } from '@angular/router';
import { NotificationsService } from 'angular2-notifications';

import { Event } from './../../../core/models/events';
import { User } from './../../../core/models/users';
import { EventsService } from './../../../core/services/events.service';

@Component({
  selector: 'app-event-create',
  templateUrl: './event-create.component.html',
  styleUrls: ['./event-create.component.css']
})
export class EventCreateComponent implements OnInit {
    model: Event;
    events: Event[];
    title: string;
    @Output('create') emitter: EventEmitter<Event> = new EventEmitter<Event>();

    constructor(
        private eventsService: EventsService,
        private router: Router,
        private notificationsService: NotificationsService
    ) { }

    ngOnInit() {
        this.model = new Event(0, '','', '', '', '', '');
    }

    create() {
        const event = new Event(++this.eventsService.lastId, this.model.title, this.model.when,'upcoming', this.model.where,
           this.model.purpose  , this.model.image_url);

        if (!event) { return; }
        this.eventsService.add(event)
            .then(() => {
                this.notificationsService.success('Success', 'Event ' + event.title + ' has been successfully created!');
                this.router.navigate(['./events']);
            });

        this.emitter.emit(event);
    }
}