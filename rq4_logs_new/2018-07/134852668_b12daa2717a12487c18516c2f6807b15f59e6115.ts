import { Pipe, PipeTransform } from '@angular/core';

/**
 * Generated class for the LaunchFilterPipe pipe.
 *
 * See https://angular.io/api/core/Pipe for more info on Angular Pipes.
 */
@Pipe({
  name: 'launchFilter',
})
export class LaunchFilterPipe implements PipeTransform {

    /**
     * Show launches according to filters
     * @param items 
     * @param filters 
     */
    transform(items: any[], filters: any): any[] {
        if (!items) {
          return [];
        }

        if (!filters) {
          return items;
        }

        // Filter rocket name
        if(filters.rocket && filters.rocket != "all") {
          filters.rocket = filters.rocket.toLowerCase();
          items =  items.filter(item => {
            return item.rocket.rocket_id.toLowerCase().includes(filters.rocket);
          });
        }

        return items;
    }
}