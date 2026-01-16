import {patchState, signalStore, withComputed, withMethods, withState} from '@ngrx/signals';
import {computed} from '@angular/core';

export interface DateState {
  startDate: Date;
  endDate: Date;
  maxDate: Date;
  rangeToggle: boolean;
}

const initialState: DateState = {
  startDate: new Date(),
  endDate: new Date(),
  maxDate: new Date(),
  rangeToggle: false
}

export const SelectedDateState = signalStore(
  { providedIn: 'root' },
   withState(initialState),
   withMethods((store) => {
     return {
       selectDate(date: Date): void {
         patchState(store, {startDate: date, endDate: date});
       },
       selectDateRange(from: Date, to: Date): void {
         patchState(store, {startDate: from, endDate: to});
       },
       resetDate() {
         patchState(store, {startDate: new Date(), endDate: new Date()});
       },
       setRangeToggle(value: boolean): void {
         patchState(store, {rangeToggle: value});
       }
     }
   }),
   withComputed(({startDate, endDate}) => ({
     selectedRange: computed(() => {
       return {
         from: startDate(),
         to: endDate()
       };
     })
   }))
  );

export type SelectedDateState = InstanceType<typeof SelectedDateState>;