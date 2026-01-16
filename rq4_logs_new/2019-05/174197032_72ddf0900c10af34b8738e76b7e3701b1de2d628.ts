import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ReserveSeatComponent } from './reserve-seat.component';

describe('ReserveSeatComponent', () => {
  let component: ReserveSeatComponent;
  let fixture: ComponentFixture<ReserveSeatComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ReserveSeatComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ReserveSeatComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});