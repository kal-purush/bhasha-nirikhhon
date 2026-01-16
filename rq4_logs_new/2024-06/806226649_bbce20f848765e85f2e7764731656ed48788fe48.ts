import {
  DateAdapter,
  MAT_DATE_FORMATS,
  MAT_DATE_LOCALE,
  NativeDateAdapter,
} from '@angular/material/core';
import { Injectable } from '@angular/core';

const DATE_FORMATS = {
  parse: {
    dateInput: 'dd.MM.yyyy',
  },
  display: {
    dateInput: 'dd.MM.yyyy',
    monthYearLabel: 'yyyy',
    dateA11yLabel: 'dd.MM.yyyy',
    monthYearA11yLabel: 'yyyy',
  },
};

class AppDateAdapter extends NativeDateAdapter {
  public constructor(matDateLocale: string) {
    super(matDateLocale);
  }


  public override format(date: Date, displayFormat: string): string {
    if (displayFormat === 'dd.MM.yyyy') {
      return date.toLocaleDateString('de-CH', {
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
      });
    } else {
      return date.getFullYear().toString();
    }
  }
}

export const DateAdapterProviders = [
  { provide: MAT_DATE_FORMATS, useValue: DATE_FORMATS },
  {
    provide: DateAdapter,
    useClass: AppDateAdapter,
    deps: [MAT_DATE_LOCALE],
  },
];