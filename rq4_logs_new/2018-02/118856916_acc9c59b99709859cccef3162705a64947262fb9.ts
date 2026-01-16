import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { QaManagementComponent } from './qa-management.component';

describe('QaManagementComponent', () => {
  let component: QaManagementComponent;
  let fixture: ComponentFixture<QaManagementComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ QaManagementComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(QaManagementComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});