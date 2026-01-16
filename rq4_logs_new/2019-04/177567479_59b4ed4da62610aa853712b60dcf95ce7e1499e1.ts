import { Pipe, PipeTransform } from '@angular/core';

@Pipe({
  name: 'orderBy',
  pure: false,
})
export class OrderByPipe implements PipeTransform {
  transform(array, field: string, flag: boolean = true): any {
    if (!Array.isArray(array)) {
      return;
    }
    if (field !== 'number') {
      array.sort((a: any, b: any) => {
        if (a.product[field] < b.product[field]) {
          return flag ? -1 : 1;
        } else if (a.product[field] > b.product[field]) {
          return flag ? 1 : -1;
        } else {
          return 0;
        }
      });
    } else {
      array.sort((a: any, b: any) => {
        if (a[field] < b[field]) {
          return flag ? -1 : 1;
        } else if (a[field] > b[field]) {
          return flag ? 1 : -1;
        } else {
          return 0;
        }
      });
    }

    return array;
  }
}