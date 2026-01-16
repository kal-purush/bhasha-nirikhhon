import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { AdminEnrollmentHouseDetailModalComponent } from './admin-enrollment-house-detail-modal.component';

describe('AdminEnrollmentHouseDetailModalComponent', () => {
  let component: AdminEnrollmentHouseDetailModalComponent;
  let fixture: ComponentFixture<AdminEnrollmentHouseDetailModalComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ AdminEnrollmentHouseDetailModalComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(AdminEnrollmentHouseDetailModalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});