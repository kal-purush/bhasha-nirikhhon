import { Pipe, PipeTransform } from '@angular/core';
/*
 * Raise the value exponentially
 * Takes an exponent argument that defaults to 1.
 * Usage:
 *   value | alphabeticaOrder:exponent
 * Example:
 *   {{ 2 |  alphabeticaOrder:10}}
 *   formats to: 1024
*/
class kio {
  name: string;
  flags: string[];
}
@Pipe({
  name: 'alphabeticaOrder',
  pure: false
})
export class AlphabeticaOrderPipe implements PipeTransform {
  transform(arr: kio[]) {
    return arr.sort((a, b) => {
      let nameA = a.name.toUpperCase(); // ignore upper and lowercase
      let nameB = b.name.toUpperCase(); // ignore upper and lowercase
  
      if (nameA < nameB) {
        return -1;
      }
      if (nameA > nameB) {
        return 1;
      }
      return 0;
    });
  }
}