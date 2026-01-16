import { Pipe, PipeTransform } from '@angular/core';
import {DatePipe} from '@angular/common';

@Pipe({
  name: 'dateformatter'
})
export class DateformatterPipe implements PipeTransform {

  constructor(private datePipe: DatePipe){}

  transform(value: string): string {

    const date = this.datePipe.transform(new Date(Number(value)*1000));
    const day = new Date();
    const today = this.datePipe.transform(day);
    const yesterday = this.datePipe.transform(day.setDate(day.getDate()-1));
    if (date == today){
      return 'Today';
    }else if(date == yesterday){
      return 'Yesterday';
    }else{
      return date;
    }
  }

}