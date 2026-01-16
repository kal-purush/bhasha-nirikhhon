import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { AdminEnrolmentCreateComponent } from './admin-enrolment-create.component';

describe('AdminEnrolmentCreateComponent', () => {
  let component: AdminEnrolmentCreateComponent;
  let fixture: ComponentFixture<AdminEnrolmentCreateComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ AdminEnrolmentCreateComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(AdminEnrolmentCreateComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});