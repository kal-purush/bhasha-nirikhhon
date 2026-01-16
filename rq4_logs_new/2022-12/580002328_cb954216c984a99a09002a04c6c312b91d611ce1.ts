import { ComponentFixture, TestBed } from '@angular/core/testing';

import { DailyTrackerFormComponent } from './daily-tracker-form.component';

describe('DailyTrackerFormComponent', () => {
  let component: DailyTrackerFormComponent;
  let fixture: ComponentFixture<DailyTrackerFormComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ DailyTrackerFormComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(DailyTrackerFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});