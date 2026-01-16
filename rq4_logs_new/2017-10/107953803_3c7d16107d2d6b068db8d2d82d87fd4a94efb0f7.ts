import { async, ComponentFixture, fakeAsync, TestBed, tick
} from '@angular/core/testing';
import {AppModule} from "./app.module";
import {HeroesComponent} from "./heroes.component";
import {By} from "@angular/platform-browser";
import {Hero} from "./hero";
import {HeroService} from "./hero.service";

let comp: HeroesComponent;
let fixture: ComponentFixture<HeroesComponent>;
let page: Page;

describe('HeroesComponent', () => {

  beforeEach(async(() => { // We wrap it in async, because we want to compile modules and external files (like HTML) asynchronously.
    TestBed.configureTestingModule({ // TestBed configures modules, as in our AppModule, but for this test only.
      imports: [AppModule]
    }).overrideModule(AppModule, { // We are going to override some parts of the AppModule, because we don't want to use our real service, as we aren't testing the service itself here.
      remove: {
        providers: [
          HeroService
        ]
      },
      add: {
        providers: [
          { provide: HeroService, useClass: FakeHeroService }
        ]
      }
    }).compileComponents().then(() => {
      // We create a component to test using the TestBed.
      fixture = TestBed.createComponent(HeroesComponent);
      // And get the actual instance itself to do different things that we can test.
      comp = fixture.componentInstance;

      // detectChanges triggers ngOnInit, which gets triggered after initialization.
      fixture.detectChanges();

      fixture.whenStable().then(() => {
        // after getting heroes and the updated component, detectChanges updates the view (HTML).
        fixture.detectChanges();
        page = new Page();
      });
    });
  }));

  it('should display heroes', () => {
    expect(page.heroRows.length).toBeGreaterThan(0);
  });

  it('1st hero should match 1st test hero', () => {
    const expectedHero = fakeHeroes[0];
    const actualHero = page.heroRows[0].textContent;
    expect(actualHero).toContain(expectedHero.id.toString(), 'hero.id');
    expect(actualHero).toContain(expectedHero.name, 'hero.name');
  });

  it('should select hero on click', fakeAsync(() => {
    const expectedHero = fakeHeroes[1];
    const li = page.heroRows[1];
    li.click();
    tick();
    // `.toEqual` because selectedHero is clone of expectedHero; see FakeHeroService
    expect(comp.selectedHero).toEqual(expectedHero);
  }));

  it('should select hero with calling the method', () => {
    const expectedHero = fakeHeroes[1];
    comp.onSelect(expectedHero);
    // calling onSelect on the expectedHero should have changed the selectedHero property to the expectedHero.
    expect(comp.selectedHero).toEqual(expectedHero);
  });
});

class Page { // Helper class that loads rows of heroes from the HTML using the css selector 'li'.
  /** Hero line elements */
  heroRows: HTMLLIElement[];

  constructor() {
    // we can apply it functionally.
    this.heroRows    = fixture.debugElement.queryAll(By.css('li')).map(de => de.nativeElement);
  };
}

// We aren't going to use the real HeroService, but we will mock this one, since we don't want to actually test the HeroService.
export const fakeHeroes: Hero[] = [
  {id: 41, name: 'Bob'},
  {id: 42, name: 'Carol'},
  {id: 43, name: 'Ted'},
  {id: 44, name: 'Alice'},
  {id: 45, name: 'Speedy'},
  {id: 46, name: 'Stealthy'}
];
class FakeHeroService {
  getHero(id: number): Promise<Hero> {
    let hero = fakeHeroes.find(hero => hero.id === id);
    return Promise.resolve<Hero>(hero);
  }

  getHeroes(): Promise<Hero[]> {
    return Promise.resolve<Hero[]>(fakeHeroes);
  }

  // implement other fake functions to use them later, for create and delete.
}