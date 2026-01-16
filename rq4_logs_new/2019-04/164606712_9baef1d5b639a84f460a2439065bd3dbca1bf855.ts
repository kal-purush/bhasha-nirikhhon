import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { AdminCampusFormComponent } from './admin-campus-form.component';

describe('AdminCampusFormComponent', () => {
  let component: AdminCampusFormComponent;
  let fixture: ComponentFixture<AdminCampusFormComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ AdminCampusFormComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(AdminCampusFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});