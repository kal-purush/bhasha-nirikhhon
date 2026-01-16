import { Pipe, PipeTransform} from 'angular2/core';
import { Keg } from './keg.model';

@Pipe({
  name: "type"
})

export class TypePipe implements PipeTransform {
  transform(input: Keg[], args) {
    var kegType = args[0];
    if(kegType === 'Lager') {
      return input.filter((keg) => {
        return keg.beerType === kegType;
      });
    } else if (kegType === 'IPA') {
      return input.filter((keg) => {
        return keg.beerType == kegType;
      });
    } else if (kegType === 'Stout') {
      return input.filter((keg) => {
        return keg.beerType == kegType;
      });
    } else if (kegType === 'Porter') {
      return input.filter((keg) => {
        return keg.beerType == kegType;
      });
    } else if (kegType === 'Amber') {
      return input.filter((keg) => {
        return keg.beerType == kegType;
      });
    } else if (kegType === 'Pilsner') {
      return input.filter((keg) => {
        return keg.beerType == kegType;
      });
    } else if (kegType === 'Wheat') {
      return input.filter((keg) => {
        return keg.beerType == kegType;
      });
    } else if (kegType === 'Seasonal') {
      return input.filter((keg) => {
        return keg.beerType == kegType;
      });
    } else {
      return input;
    }
  }
}