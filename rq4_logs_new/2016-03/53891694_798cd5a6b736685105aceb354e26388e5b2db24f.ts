import {Component} from 'angular2/core';
import {HeroService} from "./hero.service";
import {Hero} from "./hero";
import {OnInit} from "angular2/core";
import {Router} from "angular2/router";

@Component({
    selector: 'dashboard',
    templateUrl: 'dev/dashboard.component.html',
    //template: `
    //    <h3>Top Heroes</h3>
    //    <div class="grid grid-pad">
    //        <div class="col-1-4" *ngFor="#hero of heroes" (click)="gotoDetail(hero)">
    //            <div class="module hero">
    //                {{hero.name}}
    //            </div>
    //        </div>
    //
    //    </div>
    //`,
    styleUrls: ['src/css/dashboard.component.css']
})

export class DashboardComponent implements OnInit {
    heroes : Hero[];

    constructor(private _heroService : HeroService, private _router : Router){
    }

    // action
    gotoDetail(hero : Hero) {
        // routing from a Non-Route Component
        let link = ['HeroDetail', { id: hero.id }];
        this._router.navigate(link);
    }

    // lifecycle override
    ngOnInit() {
        // get hero list - chose to put slice in service instead of .slice(1, 5) here // .slice(1, 5)
        this._heroService.getTopHeroes().then(heroList => this.heroes = heroList);
    }
}