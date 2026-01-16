import { listLazyRoutes } from '@angular/compiler/src/aot/lazy_routes';
import { Pipe, PipeTransform } from '@angular/core';

@Pipe({
  name: 'productsort'
})
export class ProductsortPipe implements PipeTransform {

  transform(list: any[], order: string): any[] {
    if (!Array.isArray(list) || !order) {
      return list;
   };

   order = order.toLocaleLowerCase();

   return list.sort((a, b) => {

      if (order == 'ar-novekvo') {
        return a.price - b.price;
      } else if (order == 'ar-csokkeno') {
        return b.price - a.price;
      } else {
        //abc
        return a.name.localeCompare(b.name);
      }

   });

  }

}