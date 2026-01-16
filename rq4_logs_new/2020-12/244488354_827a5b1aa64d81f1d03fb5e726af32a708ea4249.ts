import { Injectable } from '@angular/core';
import { APP_SETTINGS } from '../app.settings';
import { Observable } from 'rxjs/Observable';
import { catchError, map, tap, shareReplay } from 'rxjs/operators';
import { of } from 'rxjs';
import 'rxjs/add/operator/map';
import 'rxjs/add/operator/publish';
import 'rxjs/add/observable/of';
import 'rxjs/add/operator/do';
import 'rxjs/add/operator/catch';
import { HttpClient } from '@angular/common/http';
import { EventType } from '@app/interfaces/event-type';
import { Event } from '@app/interfaces/event';

@Injectable({
    providedIn: 'root',
})
export class EventTypeService {
    eventTypes$: Observable<any>;
    public eventTypes: any;

    // this is a cache to store the event types with corresponding event lists
    // it allows us to prevent re-requesting an event type's event list once it has already been requested
    eventsByTypeCache: { [key: number]: Observable<EventType> } = {};

    constructor(private httpClient: HttpClient) {
        this.eventTypes$ = httpClient
            .get(APP_SETTINGS.EVENT_TYPES + '.json')
            .pipe(
                shareReplay(1),
                tap(() => console.log('after sharing'))
            );
    }

    // this function unsed here, but goes into your component where you want the event types
    public getEventTypes() {
        this.eventTypes$.subscribe((data) => (this.eventTypes = data));
    }

    getEventsByEventType(id: number): Observable<any> {
        console.log('cache: ' + this.eventsByTypeCache);
        this.eventsByTypeCache[id] =
            this.eventsByTypeCache[id] || this.fetchEventsByEventType(id);
        return this.eventsByTypeCache[id];
    }

    fetchEventsByEventType(id: number) {
        return this.httpClient
            .get(APP_SETTINGS.EVENT_TYPES + '/' + id + '/Events.json')
            .map((rawData) => this.mapEvents(rawData, id))
            .publish()
            .refCount();
    }

    mapEvents(rawData, id): EventType {
        return { event_type_id: id, events: rawData };
    }

    notify(id) {
        console.log(`fetch events by type: ${id}`);
    }

    // public getEventTypes(): Observable<any> {
    //     return this.httpClient.get(APP_SETTINGS.EVENT_TYPES).pipe(
    //         tap((response) => {
    //             console.log('Network list response recieved: ' + response);
    //             return response;
    //         }),
    //         catchError(this.handleError<any>('getEventTypes', []))
    //     );
    // }

    /**
     * Handle Http operation that failed.
     * Let the app continue.
     * @param operation - name of the operation that failed
     * @param result - optional value to return as the observable result
     */
    /* istanbul ignore next */
    private handleError<T>(operation = 'operation', result?: T) {
        return (error: any): Observable<T> => {
            // TODO: send the error to remote logging infrastructure
            console.error(error); // log to console instead

            // TODO: better job of transforming error for user consumption
            // Consider creating a message service for this (https://angular.io/tutorial/toh-pt4)
            console.log(`${operation} failed: ${error.message}`);

            // Let the app keep running by returning an empty result.
            return of(result as T);
        };
    }
}