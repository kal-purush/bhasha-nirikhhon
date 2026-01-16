import {Pipe, PipeTransform} from '@angular/core';
import {Events} from './event.model';

@Pipe({
  name:'eventFilter'
})
export class EventFilterPipe implements PipeTransform{
  transform(event$:Events[], searchTerm: string): Events[] {
    if (!event$ || !searchTerm){
      return event$;
    }
    return event$.filter(event =>
    event.event_name.toLowerCase().indexOf(searchTerm.toLowerCase())!== -1);
  }
}