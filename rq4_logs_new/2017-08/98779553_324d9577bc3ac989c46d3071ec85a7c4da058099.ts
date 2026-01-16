import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ScheduleAssignComponent } from './schedule-assign.component';

describe('ScheduleAssignComponent', () => {
  let component: ScheduleAssignComponent;
  let fixture: ComponentFixture<ScheduleAssignComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ScheduleAssignComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ScheduleAssignComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});