import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { CarReservationListComponent } from './car-reservation-list.component';

describe('CarReservationListComponent', () => {
  let component: CarReservationListComponent;
  let fixture: ComponentFixture<CarReservationListComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ CarReservationListComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(CarReservationListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});