import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { PersonalScheduleShiftComponent } from './personal-schedule-shift.component';

describe('PersonalScheduleShiftComponent', () => {
  let component: PersonalScheduleShiftComponent;
  let fixture: ComponentFixture<PersonalScheduleShiftComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ PersonalScheduleShiftComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(PersonalScheduleShiftComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});