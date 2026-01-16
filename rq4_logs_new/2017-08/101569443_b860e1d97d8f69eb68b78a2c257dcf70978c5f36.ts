import { Injectable, InjectionToken, Optional, Inject } from '@angular/core';
import { Http, Response, Headers, RequestOptions } from '@angular/http';
import { Effect, Actions } from '@ngrx/effects';
import { Action } from '@ngrx/store';
import { Observable } from 'rxjs/Observable';
import 'rxjs/add/operator/catch';
import 'rxjs/add/operator/map';
import 'rxjs/add/operator/switchMap';
import 'rxjs/add/operator/debounceTime';
import 'rxjs/add/operator/skip';
import 'rxjs/add/operator/takeUntil';
import { Scheduler } from 'rxjs/Scheduler';
import { async } from 'rxjs/scheduler/async';
import { empty } from 'rxjs/observable/empty';
import { of } from 'rxjs/observable/of';
import { GetDocAction, ACTION_TYPES, LoadDocAction, App_Actions } from '../store/actions';
import { Query, SearchData, Document, User } from '../model/models';
import { EMPTY_DOC } from '../store/application-state';
import { environment } from '../../environments/environment';

const SERVICE_URL = `${environment.serviceBaseURL}/view`;
const PAGE_SIZE = 10;

export const VIEW_DOC_DEBOUNCE = new InjectionToken<number>('Search Debounce');
export const VIEW_DOC_SCHEDULER = new InjectionToken<Scheduler>(
  'Load Scheduler'
);

@Injectable()
export class ViewDocEffect {
  @Effect()
  getDoc$: Observable<Action> = this.actions$
    .ofType<GetDocAction>(ACTION_TYPES.GET_DOC)
    .debounceTime(this.debounce, this.scheduler || async)
    .map(action => action.url)
    .switchMap(url => {
      if (!url) {
        return empty();
      }

      const nextLoad$ = this.actions$.ofType(ACTION_TYPES.GET_DOC).skip(1);

      const serviceUrl = `${SERVICE_URL}?url=${url}`;

      return this.http.get(serviceUrl)
        .takeUntil(nextLoad$)
        .map((result: any) => {
          const doc = result.json();

          return new LoadDocAction(this.getDocumetFromServerResult(doc));
        })
        .catch((err) => {
          console.error(err);
          return of(new LoadDocAction (EMPTY_DOC));
        });
    });

  constructor(
    private actions$: Actions,
    private http: Http,
    @Optional()
    @Inject(VIEW_DOC_DEBOUNCE)
    private debounce: number = 300,
    @Optional()
    @Inject(VIEW_DOC_SCHEDULER)
    private scheduler: Scheduler
  ) {}

  getDocumetFromServerResult(doc): Document {
    const document: Document = EMPTY_DOC;
    if (!doc && !doc.length) {
      return document;
    }
    const content = doc[0].content;

    document.title = content.title;
    document.text = content.text;
    document.tags = content.tags;
    document.owner = { id: content.owner.id, userName: content.owner.userName, displayName: content.owner.displayName };

    return document;
  }
}