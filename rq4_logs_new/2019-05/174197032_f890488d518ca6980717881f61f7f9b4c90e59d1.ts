import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { CarDiscountsComponent } from './car-discounts.component';

describe('CarDiscountsComponent', () => {
  let component: CarDiscountsComponent;
  let fixture: ComponentFixture<CarDiscountsComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ CarDiscountsComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(CarDiscountsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});