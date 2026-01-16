import { ComponentFixture, TestBed } from '@angular/core/testing';
import { By }              from '@angular/platform-browser';
import { DebugElement }    from '@angular/core';

import * as moment from 'moment';

import { STATES, CalendarState } from '../../services';
import { CalendarDayComponent } from '../day';
import { CalendarMonthComponent } from '../month';
import { CalendarWeekDaysComponent } from '../week-days';
import { CalendarComponent } from './calendar.component';

describe('calendar component', () => {
  let comp:    CalendarComponent;
  let fixture: ComponentFixture<CalendarComponent>;
  let de:      DebugElement;
  let el:      HTMLElement;

  let state: CalendarState;
  const date = moment();
  const validRange: any = {};

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [
        CalendarDayComponent,
        CalendarMonthComponent,
        CalendarWeekDaysComponent,
        CalendarComponent
      ]
    });

    fixture = TestBed.createComponent(CalendarComponent);

    state = new CalendarState([date], STATES.AVAILABLE);
    comp = fixture.componentInstance;

    comp.state = state;
    comp.config = { validRange };
    fixture.detectChanges();
  });


  describe('state function', () => {
    it('set the stateFn as state.get(date)', () => {
      expect(comp.stateFn(date)).toEqual([STATES.AVAILABLE]);
    });

    it('set the stateFn using validRange from and to', () => {
      validRange.from = moment().add(1, 'day');
      validRange.to = moment().add(2, 'day');
      const myDate = moment();
      expect(comp.stateFn(myDate)).toEqual([STATES.DISABLED]);
    });

    it('set the stateFn using validRange from without to', () => {
      validRange.from = moment().add(1, 'day');
      delete validRange.to;
      const myDate1 = moment();
      const myDate2 = moment().add(1, 'year');
      state.set(myDate2, STATES.AVAILABLE);

      expect(comp.stateFn(myDate1)).toEqual([STATES.DISABLED]);
      expect(comp.stateFn(myDate2)).toEqual([STATES.AVAILABLE]);
    });

    it('set the stateFn using validRange to without from', () => {
      delete validRange.from;
      validRange.to = moment().add(2, 'day');

      const myDate1 = moment();
      const myDate2 = moment().add(3, 'day');
      state.set(myDate1, STATES.AVAILABLE);

      expect(comp.stateFn(myDate1)).toEqual([STATES.AVAILABLE]);
      expect(comp.stateFn(myDate2)).toEqual([STATES.DISABLED]);
    });
  });
});