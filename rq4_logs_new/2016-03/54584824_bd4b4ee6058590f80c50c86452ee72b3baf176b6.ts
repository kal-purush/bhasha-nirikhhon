import { Pipe, PipeTransform } from "angular2/core";
import { Keg } from './keg.model';

@Pipe({
  name: "empty",
  pure: false
})

export class TextColorPipe implements PipeTransform {
  transform(input: Keg[], args) {
    var textColor = { color: "green" };
    var fillLevel = keg.kegLevel;
    if (fillLevel <= 50) {
      textColor = { color: "yellow" }
    } else if (fillLevel <= 31) {
      textColor = { color: "red"}
    } else if (fillLevel = 0) {
      textColor = { color: "white" }
    }
  }
}