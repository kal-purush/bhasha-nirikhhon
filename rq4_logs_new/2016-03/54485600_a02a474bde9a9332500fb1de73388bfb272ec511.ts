import {Pipe, PipeTransform} from 'angular2/core';
import {Task} from './task.model';

@Pipe({
  name: "done",
  pure: false
})
export class DonePipe implements PipeTransform {
  transform(input: Task[], args) {
    var desiredDoneState = args[0];
    if(desiredDoneState === "done") {
      return input.filter((task) => {
        return task.done;
      });
    } else if (desiredDoneState === "notDone") {
      return input.filter((task) => {
        return !task.done;
      });
    } else {
      return input;
    }
  }
}