import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { CalendarInlineComponent } from './calendar-inline.component';

describe('CalendarInlineComponent', () => {
  let component: CalendarInlineComponent;
  let fixture: ComponentFixture<CalendarInlineComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ CalendarInlineComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(CalendarInlineComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});