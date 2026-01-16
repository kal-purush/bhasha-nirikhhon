import { Pipe, PipeTransform } from '@angular/core';

@Pipe({
    name: 'joinCommaKeys'
})
export class JoinCommaKeysPipe implements PipeTransform {
    transform(input: any, args: any[]): any {
      let keys = [];
      for (let k in input) {
        keys.push(k);
      }
      return keys.join(', ');
    }
}