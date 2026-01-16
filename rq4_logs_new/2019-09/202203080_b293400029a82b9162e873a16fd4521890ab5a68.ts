import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { DisplayPrayerComponent } from './display-prayer.component';

describe('DisplayPrayerComponent', () => {
  let component: DisplayPrayerComponent;
  let fixture: ComponentFixture<DisplayPrayerComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ DisplayPrayerComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(DisplayPrayerComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});