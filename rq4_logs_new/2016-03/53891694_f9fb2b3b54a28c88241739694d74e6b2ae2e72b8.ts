import {Component} from 'angular2/core';
import {HeroDetailComponent} from "./hero-detail.component";
import {Hero} from './hero';
import {HeroService} from "./hero.service";
import {OnInit} from "angular2/core";

@Component({
    selector: 'app',
    template: `
        <h1>{{title}}</h1>
        <ul class="heroes">
            <li *ngFor="#hero of heroes" (click)="onSelect(hero)" [class.selected]="hero === selectedHero">
                <span class="badge">{{hero.id}}</span> {{hero.name}}
            </li>
        </ul>
        <hero-detail [hero]="selectedHero" ></hero-detail>
    `,
    directives: [HeroDetailComponent],
    providers: [HeroService],
    styles:[`
          .selected {
            background-color: #CFD8DC !important;
            color: white;
          }
          .heroes {
            margin: 0 0 2em 0;
            list-style-type: none;
            padding: 0;
            width: 10em;
          }
          .heroes li {
            cursor: pointer;
            position: relative;
            left: 0;
            background-color: #EEE;
            margin: .5em;
            padding: .3em 0;
            height: 1.6em;
            border-radius: 4px;
          }
          .heroes li.selected:hover {
            background-color: #BBD8DC !important;
            color: white;
          }
          .heroes li:hover {
            color: #607D8B;
            background-color: #DDD;
            left: .1em;
          }
          .heroes .text {
            position: relative;
            top: -3px;
          }
          .heroes .badge {
            display: inline-block;
            font-size: small;
            color: white;
            padding: 0.8em 0.7em 0 0.7em;
            background-color: #607D8B;
            line-height: 1em;
            position: relative;
            left: -1px;
            top: -4px;
            height: 1.8em;
            margin-right: .8em;
            border-radius: 4px 0 0 4px;
          }
    `]

})

export class AppComponent implements OnInit {
    public title = 'Tour Of Heroes';
    public heroes : Hero[];
    selectedHero : Hero;

    constructor(private _heroService : HeroService){
    }

    onSelect(hero : Hero){
        this.selectedHero = hero;
    }

    ngOnInit(){
        // if there was more logic we could call another function here instead of inline
        // this.heroes = this._heroService.getHeroes();
        this.getHeroes();
    }

    private getHeroes():void {
        // separated into new function to show how to deal with promises
        this._heroService.getHeroes().then(heroList => this.heroes = heroList);
    }
}

