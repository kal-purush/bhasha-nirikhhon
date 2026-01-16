import { Injectable } from "@angular/core";
import { catchError } from 'rxjs/operators';
import { of } from 'rxjs';
import { Store } from '@ngrx/store';

import { TransferHttpService } from '../services/transferHttp.service';
import { AppState } from '../app.reducers';
import { SetError } from '../components/notification/notification.actions';

const urls = {
  posts: '/posts',
  recentPosts: '/recentPosts',
  post: '/post',
  tabs: '/countries',
  swiperPhotos: '/swiperphotos'
};

@Injectable()
export class ApiClient {

  constructor(
    private transferHttpService: TransferHttpService,
    private store: Store<AppState>
  ) { }

  getRecentPosts() {
    return this.transferHttpService.get(urls.recentPosts);
  }

  getPosts(tab: string) {
    return this.transferHttpService.get(urls.posts, {
      params: {tab}
    })
      .pipe(
        catchError((response: any) => {
          this.store.dispatch(new SetError({message: 'Couldnt load posts'}));
          return of(response);
        })
      );
  }

  getPost(id: string) {
    return this.transferHttpService.get(`${urls.post}/${id}`)
      .pipe(
        catchError((response: any) => {
          this.store.dispatch(new SetError({message: 'Couldnt load the post'}));
          return of(response);
        })
      );
  }

  getTabs() {
    return this.transferHttpService.get(urls.tabs)
      .pipe(
        catchError((response: any) => {
          this.store.dispatch(new SetError({message: 'Couldnt load tabs'}));
          return of(response);
        })
      );
  }

  getPhotos() {
    return this.transferHttpService.get(urls.swiperPhotos)
      .pipe(
        catchError((response: any) => {
          this.store.dispatch(new SetError({message: 'Couldnt load photos'}));
          return of(response);
        })
      );
  }

}