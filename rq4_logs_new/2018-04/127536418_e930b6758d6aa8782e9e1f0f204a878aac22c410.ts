import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { CoffeeHousesMainComponent } from './coffee-houses-main.component';

describe('CoffeeHousesMainComponent', () => {
  let component: CoffeeHousesMainComponent;
  let fixture: ComponentFixture<CoffeeHousesMainComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ CoffeeHousesMainComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(CoffeeHousesMainComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});