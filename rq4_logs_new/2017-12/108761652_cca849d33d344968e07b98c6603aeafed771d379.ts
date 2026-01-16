import {NgModule, Optional, SkipSelf} from '@angular/core';

import {PostsService} from '../../services/posts.service';
import {UsersService} from '../../services/users.service';
import {CategoriesService} from '../../services/categories.service';
import {ImagesService} from '../../services/images.service';
import {CommentsService} from '../../services/comments.service';

@NgModule({
  providers: [
    UsersService,
    PostsService,
    CategoriesService,
    ImagesService,
    CommentsService
  ]
})

export class CoreServicesModule {
  constructor (@Optional() @SkipSelf() core: CoreServicesModule) {
    if (core) {
      throw (new Error('CoreServiceModule must not be imported twice'));
    }
  }
}