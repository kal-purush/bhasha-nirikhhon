import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { LockerRoomTalkFormComponent } from './locker-room-talk-form.component';

describe('LockerRoomTalkFormComponent', () => {
  let component: LockerRoomTalkFormComponent;
  let fixture: ComponentFixture<LockerRoomTalkFormComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ LockerRoomTalkFormComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(LockerRoomTalkFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});