import { Component, OnInit } from '@angular/core';
import { Post } from '@app/shared/model';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';

@Component({
  selector: 'bloggers',
  templateUrl: './blogger.component.html'
})
export class BloggerComponent implements OnInit {

  blogger = 'Jecelyn';
  posts: Post[];

  constructor(private _http: HttpClient) {
  }

  ngOnInit() {
    this.getPostsByBlogger()
      .subscribe(x => this.posts = x);
  }

  getPostsByBlogger(): Observable<Array<Post>> {
    const url = 'assets/data/mock-posts.json';
    return this._http.get<Array<Post>>(url);
    // .map(x => x.json());
  }
}