import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Event } from '../models/events';
import { Venue } from '../models/venues';
import { map, Observable, of } from 'rxjs';

@Injectable({ providedIn: 'root' })
export class EventService {
  private venuesUrl = 'assets/data/venues.json';
  private cachedEvents: Event[] = [];

  constructor(private http: HttpClient) {}

  getAllEvents(): Observable<Event[]> {
    if (this.cachedEvents.length > 0) {
      return of(this.cachedEvents);
    }

    return this.http.get<{ venues: Venue[] }>(this.venuesUrl).pipe(
      map(response => {
        this.cachedEvents = response.venues.flatMap(v => v.events);
        return this.cachedEvents;
      })
    );
  }

  getEventBySlug(slug: string): Observable<Event | null> {
    return this.getAllEvents().pipe(
      map(events => events.find(event =>
        event.name.toLowerCase().replace(/\s+/g, '-') === slug.toLowerCase()
      ) || null)
    );
  }
}