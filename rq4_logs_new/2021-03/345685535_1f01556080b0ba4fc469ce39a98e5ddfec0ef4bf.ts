import { Pipe, PipeTransform } from '@angular/core';
import { User } from '../model/user';

@Pipe({
  name: 'filter'
})
export class FilterPipe implements PipeTransform {

  transform(value: any[] | null, phrase: string | number | boolean): any[] | null {
    if (!Array.isArray(value) || !phrase) {
      return value;
    }
    phrase = typeof phrase !== 'number' ? ('' + phrase).toLowerCase() : phrase;

    //Még nem működik!!!!!!!!!!
    return value.filter(item => {
      if (typeof item === 'number' && typeof phrase === 'number') {
        return item === phrase;

      } else {
        for (let i = 0; i < item.length; i++) {
          return item[i].toLowerCase().includes((phrase as string));
        }

      }
    })
  }

}