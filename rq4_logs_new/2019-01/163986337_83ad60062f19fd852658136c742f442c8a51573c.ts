import { Component, OnInit, OnDestroy } from '@angular/core';
import { Store } from '@ngrx/store';
import { Observable } from 'rxjs';
import { flatMap, takeWhile } from 'rxjs/operators';
import { Post } from '../postsList/postsList.model';
import { AppState } from '../../../app/app.reducers';
import { SinglePostService } from './singlePost.service';
import { ActivatedRoute } from '@angular/router';
import { SetPost } from './singlePost.actions';

@Component({
  selector: 'singlePost-component',
  styleUrls: ['../postsList/postsList.component.scss', './singlePost.component.scss'],
  templateUrl: './singlePost.component.html'
})
export class SinglePostComponent implements OnInit, OnDestroy {

  post$: Observable<Post>;

  private alive = true;

  constructor(
    private store: Store<AppState>,
    private route: ActivatedRoute,
    private singlePostService: SinglePostService
  ) { 
    this.post$ = this.store.select(state => state.singlePost.post);
  }

  ngOnInit() {
    this.getPostOnNewRequest();
  }

  ngOnDestroy() {
    this.alive = false;
  }

  private getPostOnNewRequest() {
    this.route.paramMap
      .pipe(
        takeWhile(() => this.alive),
        flatMap(params => this.singlePostService.getPost(params.get('id')))
      )
      .subscribe((post: Post) => {
        this.store.dispatch(new SetPost(post));
      });
  }

}