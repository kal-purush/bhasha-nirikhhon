import { Action } from '@ngrx/store';
import { Standings } from 'models/standings.model';

export const LOAD_STANDINGS = '[Standings] Load Standings';
export const LOAD_STANDINGS_SUCCESS = '[Standings] Load Standings Success';
export const LOAD_STANDINGS_FAIL = '[Standings] Load Standings Fail';

export class LoadStandings implements Action {
    readonly type = LOAD_STANDINGS;
}

export class LoadStandingsSuccess implements Action {
    readonly type = LOAD_STANDINGS_SUCCESS;

    constructor(public payload: Standings[]) {}
}

export class LoadStandingsFail implements Action {
    readonly type = LOAD_STANDINGS_FAIL;

    constructor(public payload: any) {}
}

export type StandingsAction = LoadStandings | LoadStandingsSuccess | LoadStandingsFail;