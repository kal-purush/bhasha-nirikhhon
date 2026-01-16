import { Pipe, PipeTransform } from '@angular/core';

import { Team } from './../models/team';

function sortAsc(a: string, b: string) {
    if (isNaN(+a) && isNaN(+b)) {
        return a.localeCompare(b);
    }

    return +a - +b;
}

@Pipe({ name: 'sortTeams', pure: true })
export class SortTaemsPipe implements PipeTransform {
    TextStreamBase: Team;

    transform(items: any[], sortBy: string = 'Rating', isDesc: boolean = true): Team[] {
        if (!items) { return; }

        let sortFunc = sortAsc;
        let sortingProperties = ['Name', 'Date'];

        switch(sortBy){
            case sortingProperties[0]: sortBy = 'name'; break;
            case sortingProperties[1]: sortBy = 'createdAt'; break;
        }
            return items.sort((a, b) => {
                return sortFunc(a[sortBy].toString(), b[sortBy].toString());
            });
    }
}