import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { EventDetailFinishActivityComponent } from './event-detail-finish-activity.component';

describe('EventDetailFinishActivityComponent', () => {
  let component: EventDetailFinishActivityComponent;
  let fixture: ComponentFixture<EventDetailFinishActivityComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ EventDetailFinishActivityComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(EventDetailFinishActivityComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});