import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { EventcreatorComponent } from './eventcreator.component';

describe('EventcreatorComponent', () => {
  let component: EventcreatorComponent;
  let fixture: ComponentFixture<EventcreatorComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ EventcreatorComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(EventcreatorComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});