import { Pipe, PipeTransform } from '@angular/core';

@Pipe({
  name: 'sortBy'
})
export class SortPipe implements PipeTransform {

  transform(array: Array<any>, prop: string, key: string): Array<any> {
    if (array && array.length) {
      let sortFn;
      switch (key) {
        case ('ascending'): sortFn = (a: any, b: any) => a[prop] < b[prop]; break;
        case ('descending'): sortFn = (a: any, b: any) => a[prop] > b[prop]; break;
        case ('alphabetical'): sortFn = (a: any, b: any) => String(a[prop]).localeCompare(b[prop]); break;
        default: break;
      }
      return array.sort(sortFn);
    }
  }

}