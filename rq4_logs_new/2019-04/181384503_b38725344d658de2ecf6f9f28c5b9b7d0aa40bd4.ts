import { Pipe, PipeTransform } from '@angular/core';

@Pipe({ name: 'upper' })
export class UppercasePipe implements PipeTransform {
  transform(value:string):String {
    //   const transforemdValue=value.toUpperCase();
    //   return transforemdValue;
    debugger;
    return value.toUpperCase();
  }
}