import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { RateRoomAndHotelComponent } from './rate-room-and-hotel.component';

describe('RateRoomAndHotelComponent', () => {
  let component: RateRoomAndHotelComponent;
  let fixture: ComponentFixture<RateRoomAndHotelComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ RateRoomAndHotelComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(RateRoomAndHotelComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});