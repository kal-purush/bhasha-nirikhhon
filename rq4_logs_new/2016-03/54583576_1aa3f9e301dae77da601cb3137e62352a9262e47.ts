import {Pipe, PipeTransform} from 'angular2/core';
import {Keg} from './keg.model';

@Pipe({
  name: "low",
  pure: false
})

export class LowPipe implements PipeTransform {
  transform(input: Keg[], args) {
    var desiredLowState = args[0];
    if(desiredLowState === "none") {
      return input;
    } else if (desiredLowState === "low") {
      return input.filter((keg) => {
        return (keg.pints <= 10);
      });
    } else if (desiredLowState === "notLow") {
      return input.filter((keg) => {
        return (keg.pints > 10);
      });
    } else {
      return input;
    }
  }
}