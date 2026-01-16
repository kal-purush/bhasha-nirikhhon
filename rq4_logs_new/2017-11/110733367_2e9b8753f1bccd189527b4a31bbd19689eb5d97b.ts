import { Action } from '@ngrx/store';

/* tslint:disable */
export const INCREMENT: string = '[State management] INCREMENT';
export const EVENT_FROM_EFFECT = '[State management] EVENT_FROM EVENT';
/* tslint:enable */

export class IncrementAction implements Action {
  public readonly type = INCREMENT; // tslint:disable-line
}

export class EventFromEffectAction implements Action {
  public readonly type = EVENT_FROM_EFFECT; // tslint:disable-line
}

export type Actions =
  IncrementAction |
  EventFromEffectAction;