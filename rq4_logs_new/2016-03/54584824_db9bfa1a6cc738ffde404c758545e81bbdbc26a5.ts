import { Pipe, PipeTransform } from "angular2/core";
import { Keg } from './keg.model';

@Pipe({
  name: "empty",
  pure: false
})

export class EmptyPipe implements PipeTransform {
  transform(input: Keg[], args) {
    var kegBlown = args[0];
    if(kegBlown === "empty") {
      return input.filter((keg) => {
        return keg.empty;
      });
    } else if (kegBlown === "notEmpty") {
      return input.filter((keg) => {
        return !keg.empty;
      });
    } else {
      return input;
    }
  }
}