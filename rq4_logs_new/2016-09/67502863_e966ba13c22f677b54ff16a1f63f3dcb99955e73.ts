
import {Injectable} from '@angular/core';


@Injectable()
export class CalendarService {
  constructor() {
    console.log('CalendarService');
  }

  getEvents(day) {
    let events = [
      {
        name:'Plan for weekend'
      },
      {
        name:'Book Tickets for Movie'
      },
      {
        name:'Meeting at 4 pm'
      },
      {
        name:'Dinner Party '
      }
    ]

    return events;
  }
}