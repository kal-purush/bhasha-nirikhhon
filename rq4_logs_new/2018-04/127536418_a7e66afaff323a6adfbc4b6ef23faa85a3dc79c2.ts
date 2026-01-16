import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { CoffeeDrinkComponent } from './coffee-drink.component';

describe('CoffeeDrinkComponent', () => {
  let component: CoffeeDrinkComponent;
  let fixture: ComponentFixture<CoffeeDrinkComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ CoffeeDrinkComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(CoffeeDrinkComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});