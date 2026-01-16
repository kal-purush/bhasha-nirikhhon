import { Pipe, PipeTransform } from '@angular/core';
import { PlayerModel } from 'src/app/+state/entities/player/player.model';

@Pipe({
  name: 'search'
})
export class SearchPipe implements PipeTransform {

  transform(value: any, term: string): any {
    if (!term) return value;

    return value.filter((player: PlayerModel) => (new RegExp(term, 'gi').test(player.name)));
  }

}