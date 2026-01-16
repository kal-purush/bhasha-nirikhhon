import { Pipe, PipeTransform } from '@angular/core';

@Pipe({
  name: 'postDateOrder'
})
export class PostDateOrderPipe implements PipeTransform {

  transform(users: any[], order: boolean): any {
    if( !users ){
      return users;
    }

    if (order) {
      
      return users.sort( (a, b) => {
        let aD = new Date(a.date);
        let bD = new Date(b.date);
        if (aD.getTime() > bD.getTime()) {
          return -1;
        }
        if (aD.getTime() < bD.getTime()) {
          return 1;
        }
        return 0;
      })
    } else {
      return users;
    }
    
    
  }
}