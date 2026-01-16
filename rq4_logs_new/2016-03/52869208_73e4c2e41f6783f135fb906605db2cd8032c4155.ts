import { Component } from 'angular2/core';
import { RouteConfig, ROUTER_DIRECTIVES, ROUTER_PROVIDERS } from 'angular2/router';
// import { HeroService } from './hero.service';
// import { DashboardComponent } from './dashboard.component';
// import { GuildComponent } from './guild.component';
// import { HeroDetailComponent } from './hero-detail.component';
// import { DuelComponent } from './duel.component';
// import { DungeonComponent } from './dungeon.component';
// import { MonsterService } from './monster.service';
// import { DungeonService } from './dungeon.service';
// import { GuildService } from './guild.service';

@Component({
    selector: 'my-app',
    templateUrl : 'app/app.component.html',
    styleUrls: ['app/app.component.css'],
    directives: [ROUTER_DIRECTIVES],
    providers: [
        ROUTER_PROVIDERS,
        // HeroService,
        // MonsterService,
        // DungeonService,
        // GuildService
    ]

})
@RouteConfig([
    // {
    //     path: '/dashboard',
    //     name: 'Dashboard',
    //     component: DashboardComponent,
    //     useAsDefault: true
    // },
    // {
    //     path: '/detail/:id',
    //     name: 'HeroDetail',
    //     component: HeroDetailComponent
    // },
    // {
    //     path: '/guild',
    //     name: 'Guild',
    //     component: GuildComponent
    // },
    // {
    //     path: '/duel',
    //     name: 'Duel',
    //     component: DuelComponent
    // },
    // {
    //     path: '/dungeon',
    //     name: 'Dungeon',
    //     component: DungeonComponent
    // }


])

export class AppComponent {
    title = 'Guild Simulator';
}