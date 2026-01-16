import { ComponentFixture, TestBed } from '@angular/core/testing';

import { SchedulerFormComponent } from './scheduler-form.component';

describe('SchedulerFormComponent', () => {
  let component: SchedulerFormComponent;
  let fixture: ComponentFixture<SchedulerFormComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ SchedulerFormComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(SchedulerFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});