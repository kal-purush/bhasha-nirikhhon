import { Pipe } from '@angular/core';

import { Post } from '../models/post';

@Pipe({ name: 'contains', pure: false })
export class ContainsPipe {
    transform(posts: Post[], keyword: string) {
        if(!posts) { return; }
        if(keyword == null || keyword.trim() == '') { return posts; }

        keyword = keyword.toLowerCase();

        return posts.filter(post => post.title.toLowerCase().indexOf(keyword) != -1
                                || post.author.toLowerCase().indexOf(keyword) != -1);
    }
}