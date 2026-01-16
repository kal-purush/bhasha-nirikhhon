import { Pipe, PipeTransform } from '@angular/core';
import { Project } from './../models/projects';

@Pipe({ name: 'filterProjects', pure: true })
export class FilterProjectsPipe implements PipeTransform {
    transform(items: Project[], filterValue: string = ''): Project[] {
        if (!items) {
            return;
        }

        if (filterValue === '') {
            return items;
        }

        return items.filter(item =>
            item.Title.toLocaleLowerCase()
                .indexOf(filterValue) > -1);
    }
}