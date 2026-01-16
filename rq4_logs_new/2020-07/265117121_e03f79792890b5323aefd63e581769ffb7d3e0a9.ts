import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { AdminContactsDetailComponent } from './admin-contacts-detail.component';

describe('AdminContactsDetailComponent', () => {
  let component: AdminContactsDetailComponent;
  let fixture: ComponentFixture<AdminContactsDetailComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ AdminContactsDetailComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(AdminContactsDetailComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});