import { Component, Pipe, PipeTransform } from 'angular2/angular2';


@Pipe({
  name: 'temperaturConverter'
})
// The work of the pipe is handled in the tranform method with our pipe's class
export class TemperaturConverterPipe implements PipeTransform {
  transform(value: number, args: any[]) {
    var places:number = args[1];
    if(value && !isNaN(value) && args[0] === 'C') {
      return value + ' °C';
    }
    if(value && !isNaN(value) && args[0] === 'F') {
      return ((value / 9 *5)+ 32).toFixed(places) + ' °F' ;
    }
    if(value && !isNaN(value) && args[0] === 'K') {
      value = 273.15 + Number(value) ;
      return value + ' K';
    }
    return;
  }
}