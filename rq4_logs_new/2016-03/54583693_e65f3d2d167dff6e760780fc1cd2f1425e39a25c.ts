import {Pipe, PipeTransform} from 'angular2/core';
import {Keg} from './keg.model';

@Pipe({
  name: "change",
  pure: false
})
export class ChangePipe implements PipeTransform {
  transform(input: Keg[], args) {
    var desiredState = args[0];
    if(desiredState === "low"){
      return input.filter((keg) =>{
        return keg.pintCount < 11;
      });
    } else {
      return input;
    }
  }
}