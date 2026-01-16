import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';
import { Observable } from 'rxjs/Observable';
import { Subject } from 'rxjs/Subject';

import { Hero } from './hero';
import { HeroSearchService } from './hero-search.service';

@Component({
    moduleId: module.id,
    selector: 'hero-search',
    templateUrl: 'hero-search.component.html',
    styleUrls: ['hero-search.component.css'],
    providers: [HeroSearchService]
})
export class HeroSearchComponent implements OnInit {
    private searchTerms = new Subject<string>();
    heroes: Observable<Hero[]>;
    
    
    constructor(
        private router: Router,
        private heroSearchService: HeroSearchService ) { }

    search(term: string): void {
        this.searchTerms.next(term);       
    }

    ngOnInit() {
        this.heroes = this.searchTerms
            .debounceTime(300)        // wait for 300ms pause in events
            .distinctUntilChanged()   // ignore if next search term is same as previous
            .switchMap(term => term   // switch to new observable each time
                // return the http search observable
                ? this.heroSearchService.search(term)
                // or the observable of empty heroes if no search term
                : Observable.of<Hero[]>([]))
            .catch(error => {
            // TODO: real error handling
            console.log(error);
            return Observable.of<Hero[]>([]);
        });
    }

    gotoDetail(hero: Hero) {
        let link = ['/detail', hero.id];
        this.router.navigate(link);
    }
}