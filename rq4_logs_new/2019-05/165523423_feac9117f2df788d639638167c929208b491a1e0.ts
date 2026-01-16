/* eslint-disable no-unused-vars */
import { put } from 'redux-saga/effects';
import { DB } from '../../firebase/firebase';
import {
  createHeroSuccess,
  deleteHeroSuccess,
  fetchHeroesStart,
  fetchHeroSuccess,
  fetchHeroesSuccess,
  fetchHeroesFail,
  updateHeroSuccess
} from './heroActions';
import Hero from '../../shared/Hero';

export function* createHeroSaga({ hero, route }: { hero: Hero; route: any }) {
  try {
    // create new entry spot in Firebase
    const newHeroRef = yield DB.ref('heroes').push();

    // set new hero in created spot
    const res = yield newHeroRef.set(hero);

    // grab unique DB id created by Firebase
    hero.id = newHeroRef.key;

    yield put(createHeroSuccess(hero));

    // redirect to track page
    yield route.push(`/track/${hero.id}/stats`);
  } catch (error) {
    // TODO: swap out action for fail
    yield console.log(error);
  }
}

export function* deleteHeroSaga({ heroId }: { heroId: number }) {
  try {
    // grab specific hero DB ref
    const heroRef = yield DB.ref(`heroes/${heroId}`);

    // delete hero
    const res = yield heroRef.remove();

    yield put(deleteHeroSuccess(heroId));
  } catch (error) {
    // TODO: add error handling
    console.log(error);
  }
}

export function* fetchHeroSaga({ heroId }: { heroId: number }) {
  try {
    // hero specif DB ref
    const heroRef = yield DB.ref(`heroes/${heroId}`);

    // retrieve snapshot of hero
    const heroData = yield heroRef.once('value');

    // pull value of of snapshot
    const hero = heroData.val();

    yield put(fetchHeroSuccess(hero));
  } catch (error) {
    // TODO: add error handling
    console.log(error);
  }
}

export function* fetchHeroesSaga() {
  yield put(fetchHeroesStart());

  try {
    // hero list specific DB reference
    const heroesRef = yield DB.ref('heroes');

    // retrieve snapshot of heroes list
    const heroesData = yield heroesRef.once('value');

    // pull values out of snapshot
    const heroes = heroesData.val();

    const heroesList = [];
    for (const key in heroes) {
      heroesList.push({ ...heroes[key], id: key });
    }

    yield put(fetchHeroesSuccess(heroesList));
  } catch (error) {
    yield put(fetchHeroesFail(error));
  }
}

export function* updateHeroSaga({ hero }: { hero: Hero }) {
  try {
    // grab specific hero DB ref
    const heroRef = yield DB.ref(`heroes/${hero.id}`);

    // set updated hero
    const res = yield heroRef.set(hero);

    yield put(updateHeroSuccess(hero));
  } catch (error) {
    // TODO: add error handling
    console.log(error);
  }
}