import { Component } from '@angular/core';
import { Router, ActivatedRoute } from '@angular/router';
import 'rxjs';
import { PostService } from '../../services/post.service';


@Component({
    selector: 'post-detail',
    templateUrl: './post-detail.component.html',
    styleUrls: ['./post-detail.component.css']
})

export class PostDetailComponent {

    title: string = 'Post detail';
    id: string;
    constructor(private _postService: PostService, private route: ActivatedRoute) {

        this.route.params.subscribe(params => {
            this.id = params['id'];
            if (this.id) {
                this._postService.posts$.map(posts => {
                    return posts.filter(post => post['_id'] == this.id)
                })
                    .subscribe(post => {
                        console.log('post2', post);
                    })
            }
        })
    }
}