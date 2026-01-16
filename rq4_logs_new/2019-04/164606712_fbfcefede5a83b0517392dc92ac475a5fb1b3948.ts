import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { AdminCampusEventListComponent } from './admin-campus-event-list.component';

describe('AdminCampusEventListComponent', () => {
  let component: AdminCampusEventListComponent;
  let fixture: ComponentFixture<AdminCampusEventListComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ AdminCampusEventListComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(AdminCampusEventListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});