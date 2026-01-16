import {
    Component,
    Input,
    OnInit,
    OnDestroy
} from "@angular/core";

import {
    Router,
    ActivatedRoute
} from "@angular/router";

import { Observable } from "rxjs/Observable";

import { Hero } from "../../../classes/hero/hero";
import { HeroService } from "../../../services/hero.service";

@Component({
    moduleId: module.id,
    selector: "hero-detail",
    templateUrl: "hero-detail.component.html"
})

export class HeroDetailComponent implements OnInit, OnDestroy {
    hero: Hero;

    private sub: any;

    constructor(
        private route: ActivatedRoute,
        private router: Router,
        private heroService: HeroService
    ) {
        console.log("HeroDetailComponent constructor");
    }

    ngOnInit() {
        console.log("HeroDetailComponent OnInit");

        this.sub = this.route.params.subscribe(params => {
            let id = +params["id"];

            this.heroService.getHero(id).then(
                hero => this.hero = hero
            );
        });
    }

    ngOnDestroy() {
        console.log("HeroDetailComponent OnDestroy");

        this.sub.unsubscribe();
    }

    gotoHeroList() {
        let heroId = this.hero ? this.hero.id : null;

        this.router.navigate(
            ["/heroes"],
            {
                queryParams: {id: `${heroId}`, foo: "foo"}
            }
        );
    }

    goBack() {
        window.history.back();

    }
}