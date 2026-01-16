import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { EvententryComponent } from './evententry.component';

describe('EvententryComponent', () => {
  let component: EvententryComponent;
  let fixture: ComponentFixture<EvententryComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ EvententryComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(EvententryComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});