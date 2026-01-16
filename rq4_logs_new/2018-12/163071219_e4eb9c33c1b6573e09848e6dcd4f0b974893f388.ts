import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { AdminIncidentsComponent } from './admin-incidents.component';

describe('AdminIncidentsComponent', () => {
  let component: AdminIncidentsComponent;
  let fixture: ComponentFixture<AdminIncidentsComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ AdminIncidentsComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(AdminIncidentsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});