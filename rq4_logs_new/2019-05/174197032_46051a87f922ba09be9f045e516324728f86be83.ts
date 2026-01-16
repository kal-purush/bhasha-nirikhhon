import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { RoomQuickReservationComponent } from './room-quick-reservation.component';

describe('RoomQuickReservationComponent', () => {
  let component: RoomQuickReservationComponent;
  let fixture: ComponentFixture<RoomQuickReservationComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ RoomQuickReservationComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(RoomQuickReservationComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});