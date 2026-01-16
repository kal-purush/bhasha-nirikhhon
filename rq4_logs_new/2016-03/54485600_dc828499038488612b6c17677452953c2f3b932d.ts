import {Pipe, PipeTransform} from 'angular2/core';
import {Task} from './task.model';

@Pipe({
  name: "priority",
  pure: false
})

export class PriorityPipe implements PipeTransform {
  transform(input: Task[], args) {
  var desiredPriorityState = args[0];
  if(desiredPriorityState === "high"){
    return input.filter((task) => {
        return task.priority === "high"
    });
  } else if (desiredPriorityState === "medium"){
    return input.filter((task) => {
      return task.priority === "medium"
    });
  } else if (desiredPriorityState === "low"){
    return input.filter((task) => {
      return task.priority === "low"
    });
  } else {
    return input;
    }
  }
}