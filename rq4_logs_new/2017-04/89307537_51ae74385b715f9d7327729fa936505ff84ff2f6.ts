import { Injectable, EventEmitter } from '@angular/core';

import { Hero } from './hero.model';

@Injectable()
export class HeroService {
  heroSelected = new EventEmitter<Hero>();

  heroes = [
    { name: 'Heroe 1', description: 'Lorem ipsum blablabla' },
    { name: 'Heroe 2', description: 'Lorem ipsum blebleble' },
    { name: 'Heroe 3', description: 'Lorem ipsum bliblibli' },
    { name: 'Heroe 4', description: 'Lorem ipsum blobloblo' },
    { name: 'Heroe 5', description: 'Lorem ipsum blublublu' },
  ];

  constructor() { }

  getHeroes(): Promise<Array<Hero>> {
    return Promise.resolve(this.heroes);
  }

}