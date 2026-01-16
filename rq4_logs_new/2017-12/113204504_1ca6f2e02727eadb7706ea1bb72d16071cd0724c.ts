import { AbstractControl } from '@angular/forms';
import * as moment from 'moment';

export const OldValidator = (minOld, maxOld) => {
  return (control: AbstractControl): { [key: string]: boolean } => {
    const input = control;
    if (!input || !minOld || !maxOld) {
      return null;
    }
    const now = moment();
    const birthDate = moment(input.value).format();
    // const diff = now.diff(birthDate, 'years');
    console.log('birthDate', birthDate, 'minOld', minOld, 'max', maxOld);
    return null;
    // if (diff > 0 && minOld <= diff && diff <= maxOld) {
    //   return null;
    // }
    // return {'Old': true};
    // return {'Old': true};
  };
};