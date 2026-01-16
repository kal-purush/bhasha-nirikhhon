import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { AppointmentStep1Component } from './appointment-step1.component';

describe('AppointmentStep1Component', () => {
  let component: AppointmentStep1Component;
  let fixture: ComponentFixture<AppointmentStep1Component>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ AppointmentStep1Component ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(AppointmentStep1Component);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});