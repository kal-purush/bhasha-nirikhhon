import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { EventregisterComponent } from './eventregister.component';

describe('EventregisterComponent', () => {
  let component: EventregisterComponent;
  let fixture: ComponentFixture<EventregisterComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ EventregisterComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(EventregisterComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});