import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { HeroCardsComponent } from './hero-cards.component';

describe('HeroCardsComponent', () => {
  let component: HeroCardsComponent;
  let fixture: ComponentFixture<HeroCardsComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ HeroCardsComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(HeroCardsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});