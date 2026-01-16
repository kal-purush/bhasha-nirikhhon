import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ViewHostEventsComponent } from './view-host-events.component';

describe('ViewHostEventsComponent', () => {
  let component: ViewHostEventsComponent;
  let fixture: ComponentFixture<ViewHostEventsComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ViewHostEventsComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ViewHostEventsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});