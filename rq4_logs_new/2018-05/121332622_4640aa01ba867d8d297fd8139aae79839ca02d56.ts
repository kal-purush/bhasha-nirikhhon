import { Pipe, PipeTransform } from '@angular/core';

@Pipe({
  name: 'filter',
  pure:false
})
export class FilterPipe implements PipeTransform {

  transform(value: any, filterString: string , propertyName: string): any {
    if(value.length===0||filterString===''||propertyName ==='')
    {
      return value;
    }
    const resultArray=[];
    for (const item in value)
    {
        if((item[propertyName]).contains(filterString))
          {
              resultArray.push(item);
          }
    }
    return resultArray;
  }

}