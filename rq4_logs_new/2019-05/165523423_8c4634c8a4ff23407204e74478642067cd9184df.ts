import Hero from '../../shared/Hero';

export const CREATE_HERO = 'CREATE_HERO';
export const CREATE_HERO_FAIL = 'CREATE_HERO_FAIL';
export const CREATE_HERO_SUCCESS = 'CREATE_HERO_SUCCESS';

export const DELETE_HERO = 'DELETE_HERO';
export const DELETE_HERO_FAIL = 'DELETE_HERO_FAIL';
export const DELETE_HERO_SUCCESS = 'DELETE_HERO_SUCCESS';

export const FETCH_HERO = 'FETCH_HERO';
export const FETCH_HERO_FAIL = 'FETCH_HERO_FAIL';
export const FETCH_HERO_SUCCESS = 'FETCH_HERO_SUCCESS';

export const FETCH_HEROES = 'FETCH_HEROES';
export const FETCH_HEROES_START = 'FETCH_HEROES_START';
export const FETCH_HEROES_FAIL = 'FETCH_HEROES_FAIL';
export const FETCH_HEROES_SUCCESS = 'FETCH_HEROES_SUCCESS';

export const GET_HERO = 'GET_HERO';

export const UPDATE_HERO = 'UPDATE_HERO';
export const UPDATE_HERO_FAIL = 'UPDATE_HERO_FAIL';
export const UPDATE_HERO_SUCCESS = 'UPDATE_HERO_SUCCESS';

export interface HeroState {
  hero: Hero;
  heroes: Hero[];
  loading: boolean;
  userId: number;
}

interface CreateHeroAction {
  type: typeof CREATE_HERO;
  hero: Hero;
  route: string;
}

interface CreateHeroFailAction {
  type: typeof CREATE_HERO_FAIL;
  error: any;
}

interface CreateHeroSuccessAction {
  type: typeof CREATE_HERO_SUCCESS;
  hero: Hero;
}

interface DeleteHeroAction {
  type: typeof DELETE_HERO;
  heroId: number;
}

interface DeleteHeroFailAction {
  type: typeof DELETE_HERO_FAIL;
  error: any;
}

interface DeleteHeroSuccessAction {
  type: typeof DELETE_HERO_SUCCESS;
  heroId: number;
}

interface FetchHeroAction {
  type: typeof FETCH_HERO;
  heroId: number;
}

interface FetchHeroFailAction {
  type: typeof FETCH_HERO_FAIL;
  error: any;
}

interface FetchHeroSuccessAction {
  type: typeof FETCH_HERO_SUCCESS;
  hero: Hero;
}

interface FetchHeroesAction {
  type: typeof FETCH_HEROES;
}

interface FetchHeroesStartAction {
  type: typeof FETCH_HEROES_START;
}

interface FetchHeroesFailAction {
  type: typeof FETCH_HEROES_FAIL;
  error: any;
}

interface FetchHeroesSuccessAction {
  type: typeof FETCH_HEROES_SUCCESS;
  heroes: Hero[];
}

interface GetHeroAction {
  type: typeof GET_HERO;
  heroId: number;
}

interface UpdateHeroAction {
  type: typeof UPDATE_HERO;
  hero: Hero;
}

interface UpdateHeroFailAction {
  type: typeof UPDATE_HERO_FAIL;
  error: any;
}

interface UpdateHeroSuccessAction {
  type: typeof UPDATE_HERO_SUCCESS;
  hero: Hero;
}

export type HeroActionTypes =
  | CreateHeroAction
  | CreateHeroFailAction
  | CreateHeroSuccessAction
  | DeleteHeroAction
  | DeleteHeroFailAction
  | DeleteHeroSuccessAction
  | FetchHeroAction
  | FetchHeroFailAction
  | FetchHeroSuccessAction
  | FetchHeroesAction
  | FetchHeroesStartAction
  | FetchHeroesFailAction
  | FetchHeroesSuccessAction
  | GetHeroAction
  | UpdateHeroAction
  | UpdateHeroFailAction
  | UpdateHeroSuccessAction;