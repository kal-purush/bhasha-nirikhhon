import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ScheduleCalendarComponent } from './schedule-calendar.component';

describe('ScheduleCalendarComponent', () => {
  let component: ScheduleCalendarComponent;
  let fixture: ComponentFixture<ScheduleCalendarComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ScheduleCalendarComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ScheduleCalendarComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});