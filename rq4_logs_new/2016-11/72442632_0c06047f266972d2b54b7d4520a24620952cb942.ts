import { Pipe, PipeTransform } from '@angular/core';

@Pipe({ 
	name: 'categoryFilter',
	pure: false
})

export class CategoryFilterPipe implements PipeTransform {
  transform(value: any, filter: string): any {

    return filter ? value.filter(attrData => 
    	attrData.category_id === filter
    ) : value
  }
}