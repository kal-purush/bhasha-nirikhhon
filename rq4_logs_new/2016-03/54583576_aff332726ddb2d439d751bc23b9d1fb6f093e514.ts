import {Pipe, PipeTransform} from 'angular2/core';
import {Keg} from './keg.model';

@Pipe({
  name: "type",
  pure: false
})

export class TypePipe implements PipeTransform {
  transform(input: Keg[], args) {
    var desiredTypeState = args[0];
    if(desiredTypeState === "none") {
      return input;
    } else {
      return input.filter((keg) => {
        return (keg.type.indexOf(args[0]) > -1);
      });
    };
  }
}