import { Pipe, PipeTransform } from '@angular/core';

/*
 * Takes an object and returns an array
*/
@Pipe({name: 'objectToArrayTransform'})
export class ObjectToArrayTransform implements PipeTransform {
  
  transform(value, args:string[]) : any {
    let keys = [];
    
    for (let key in value) {
      keys.push({key: key, value: value[key]});
    }
    
    return keys;
  }

}
