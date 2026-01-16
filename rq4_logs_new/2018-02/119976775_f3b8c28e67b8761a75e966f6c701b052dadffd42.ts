import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { CocktailPageComponent } from './cocktail-page.component';

describe('CocktailPageComponent', () => {
  let component: CocktailPageComponent;
  let fixture: ComponentFixture<CocktailPageComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ CocktailPageComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(CocktailPageComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});