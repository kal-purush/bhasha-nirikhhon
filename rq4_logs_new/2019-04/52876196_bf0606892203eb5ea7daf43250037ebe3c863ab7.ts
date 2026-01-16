import { Component, ChangeDetectionStrategy } from '@angular/core';

@Component({
  selector: 'app-demo-datepickers-filter',
  templateUrl: './filter.html',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class DemoDatepickersFilter {

  value = new Date();

  dateDisabled = (d: Date): boolean => {
    const day = d.getDay();
    // Prevent Saturday and Sunday from being selected.
    return day === 0 || day === 6;
  }

}