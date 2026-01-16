import { Pipe, PipeTransform } from 'angular2/core';
import { Restaurant } from './restaurant.model';

@Pipe({
  name: "specialty",
  pure: false
})

export class SpecialtyPipe implements PipeTransform {
  transform(input: Restaurant[], args) {
    var desiredSpecialty = args[0];
    if (desiredSpecialty === "All Restaurants") {
      return input;
    } else {
      return input.filter((restaurant) => {
        if (restaurant.specialty === desiredSpecialty) {
          return true;
        }
      });
    }
  }
}