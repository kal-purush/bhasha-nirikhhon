import {Component, View, bootstrap, CORE_DIRECTIVES} from 'angular2/angular2';
import {HeroesService} from './heroesService';
import {RestoreService} from './restoreService';
import {HeroCard} from './heroCard';


@Component({
  selector: 'hero-app'
})
@View({
  template: `
    <div>
      <ul>
        <li *ng-for="#hero of heroes">
          <hero-card [hero]="hero"></hero-card>
        </li>
      </ul>
    </div>`,
  directives: [CORE_DIRECTIVES, HeroCard]
})
export class HeroApp {
  heroes: Array<any>;
  constructor(heroesService: HeroesService) {
    this.heroes = heroesService.getHeroes();
  }
}

bootstrap(HeroApp, [HeroesService, RestoreService]);