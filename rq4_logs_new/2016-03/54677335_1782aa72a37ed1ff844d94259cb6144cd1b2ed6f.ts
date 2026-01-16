import { Pipe, PipeTransform } from 'angular2/core';
import { CD                  } from './cd.model';

@Pipe({
  name: "genre",
  pure: false
})
export class GenrePipe implements PipeTransform {
  transform(input: CD[], args) {
    var desiredGenreState = args[0];
    if(desiredGenreState === "none") {
      return input;
    } else {
      return input.filter((cd) =>  {
        return (cd.genre === args[0]);
      });
    }
  }
}