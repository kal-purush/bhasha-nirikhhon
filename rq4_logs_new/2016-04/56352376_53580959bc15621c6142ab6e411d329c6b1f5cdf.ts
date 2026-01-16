import {Http, Response} from 'angular2/http';
import {Inject, Component} from 'angular2/core';
import {PostService} from './post.service';
import {Injectable} from 'angular2/core';
import {Post} from './post.model';

@Component({
    selector: 'posts',
    providers: [PostService],
    directives: [],
    template: `
            <div>
                <button type="button" (click)='getPosts($event)'>Get Posts</button>
                    <div *ngFor="#post of posts">
                        <div>
                            <label>{{ post.Title }}</label>
                        </div>
                    </div>
            </div>`
})

@Injectable()
export class PostPage {

    public postService: PostService
    public _url: string
    public posts: Post[]
    public errorMessage: string
    
    constructor(httpService: PostService) {
        this.postService = httpService
    }
    
    getPosts () {
        
        this.posts = []
        this.postService.Get()
                         .subscribe((responseObject) => {
                                responseObject.result
                                              .map((post) => {
                                                  this.posts.push(new Post(
                                                      post.userId, 
                                                      post.id, 
                                                      post.title, 
                                                      post.body
                                                  ))
                                              })
                            },
                            error => this.errorMessage = <any>error
                         )	
                                           
        }
}