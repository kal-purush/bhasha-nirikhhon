import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ReserveRoomComponent } from './reserve-room.component';

describe('ReserveRoomComponent', () => {
  let component: ReserveRoomComponent;
  let fixture: ComponentFixture<ReserveRoomComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ReserveRoomComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ReserveRoomComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});