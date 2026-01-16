import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ConfirmAttendantPresenceComponent } from './confirm-attendant-presence.component';

describe('ConfirmAttendantPresenceComponent', () => {
  let component: ConfirmAttendantPresenceComponent;
  let fixture: ComponentFixture<ConfirmAttendantPresenceComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ConfirmAttendantPresenceComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ConfirmAttendantPresenceComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});