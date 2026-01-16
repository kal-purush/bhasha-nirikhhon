import { CommonModule } from '@angular/common';
import { TestBed } from '@angular/core/testing';
import { MatCardModule } from '@angular/material/card';
import { MatNativeDateModule } from '@angular/material/core';
import { MatDatepickerModule } from '@angular/material/datepicker';
import { RouterTestingModule } from '@angular/router/testing';
import { CalendarComponent } from './calendar.component';

describe('CalendarComponent', () => {
  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [
        RouterTestingModule,
        CommonModule,
        MatDatepickerModule,
        MatCardModule,
        MatNativeDateModule,
      ],
      declarations: [CalendarComponent],
    }).compileComponents();
  });

  it(`should create the 'CalendarComponent'`, () => {
    const fixture = TestBed.createComponent(CalendarComponent);
    const app = fixture.componentInstance;
    expect(app).toBeTruthy();
  });
});