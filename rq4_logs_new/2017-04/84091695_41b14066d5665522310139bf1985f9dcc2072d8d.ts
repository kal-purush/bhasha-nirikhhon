import {Pipe, PipeTransform} from '@angular/core';
import * as moment from 'moment';

@Pipe({
  name: 'orderBy'
})
export class OrderByPipe implements PipeTransform {

  transform(value: Array<any>, args?: any): any {
    let property = args;
    return value.sort((a, b) => {

      if (moment(a[property]).isValid()) {
        a = new Date(a[property]);
        b = new Date(b[property]);
      } else {
        a = a[property];
        b = b[property];
      }

      if (a > b) {
        return 1;
      }

      if (a < b) {
        return -1;
      }

      if (a == b) {
        return 0;
      }
    })
  }

}