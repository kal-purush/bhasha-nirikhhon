import { Injectable } from "@angular/core";

import { HERO_LIST } from "../fixtures/mock-heroes";

@Injectable()
export class HeroService {
    getHeroes() {
        //  Ultra-fast connection
        return Promise.resolve(HERO_LIST);

        // Simulate slow connection
        /**
        return new Promise<Hero[]>(
            resolve => setTimeout(
                () => resolve(HERO_LIST), 2000
            )
        );
        */
    }

    getHero(id: number) {
        return Promise.resolve(HERO_LIST)
            .then(heroes => heroes.filter(hero => hero.id === id)[0]);
    }
}