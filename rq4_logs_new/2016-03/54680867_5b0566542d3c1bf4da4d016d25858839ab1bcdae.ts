import { Pipe, PipeTransform } from 'angular2/core';
import { Album } from './album.model';

@Pipe({
  name: "genre",
  pure: false
})
export class GenrePipe implements PipeTransform {
  transform(input: Album[], args) {
    var desiredGenre = args[0];
    if(desiredGenre === "rock") {
      return input.filter((album) => {
        return album.genre === "rock";
      });
    } else if (desiredGenre === "country") {
      return input.filter((album) => {
        return album.genre === "country";
      });
    } else {
      return input;
    }
  }
}