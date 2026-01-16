'use strict';
import {Injectable} from 'angular2/angular2';
import {HEROES} from './mock-heroes';

class Hero {
	id: number;
	name: string;
}

@Injectable()
export class HeroService {
	heroes: Hero[];
	
	constructor() {
		this.heroes = HEROES;
	}
	
	getHeroes() {
		return this.heroes;
	}
}