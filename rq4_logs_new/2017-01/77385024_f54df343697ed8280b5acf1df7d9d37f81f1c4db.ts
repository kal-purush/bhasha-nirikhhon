import { Pipe, PipeTransform } from '@angular/core';
import { Team } from './../models/team';

@Pipe({ name: 'filterTeams', pure: true })
export class FilterTeamsPipe implements PipeTransform {
    transform(items: Team[], filterValue: string = ''): Team[] {
        if (!items) {
            return;
        }

        if (filterValue === '') {
            return items;
        }

        return items.filter(item =>
            item.isFilled.toLocaleLowerCase()
                .indexOf(filterValue.toLocaleLowerCase()) > -1);
    }
}