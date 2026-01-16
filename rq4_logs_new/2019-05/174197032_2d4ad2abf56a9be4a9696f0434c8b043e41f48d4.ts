import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { RoomBusinessReportsComponent } from './room-business-reports.component';

describe('RoomBusinessReportsComponent', () => {
  let component: RoomBusinessReportsComponent;
  let fixture: ComponentFixture<RoomBusinessReportsComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ RoomBusinessReportsComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(RoomBusinessReportsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});