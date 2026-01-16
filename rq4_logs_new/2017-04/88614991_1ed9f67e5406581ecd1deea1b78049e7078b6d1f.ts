import { Pipe, PipeTransform } from '@angular/core';
import { IPost } from '../models/post';
@Pipe({
    name: 'filterByPrice'
})
export class PricePipe implements PipeTransform {

    transform(posts: IPost[], args: any) {

        console.log('value', posts);
        console.log('args', args);
        if(args == undefined || args == 0) {
            return posts;
        }
        return posts.length > 0 ? posts.filter(post => {
            return post.price == args ? args : 0;
        }) : posts;
    }

}