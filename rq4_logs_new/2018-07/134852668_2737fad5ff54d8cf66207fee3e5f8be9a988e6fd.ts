import { Pipe, PipeTransform } from '@angular/core';

/**
 * Generated class for the FailFilterPipe pipe.
 *
 * See https://angular.io/api/core/Pipe for more info on Angular Pipes.
 */
@Pipe({
  name: 'failFilter',
})
export class FailFilterPipe implements PipeTransform {
  /**
   * Takes a value and makes it lowercase.
   */
  transform(items: any[], value): any[] {
    if (!items) {
      return [];
    }

    // Filter fail
    items = items.filter(item => {
      if(value == true) { 
        return true;
      } 
      else {  
        if(item.launch_success != false) {
          return true;
        }
      }
    });
    

    return items;
  }
}