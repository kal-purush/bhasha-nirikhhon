import { Injectable } from '@angular/core';
import { NewPostDisplay } from './addNewPost.model';
import { NewPost } from '../../../../shared/clients/api.model';

@Injectable()
export class AddNewPostService {

  transformNewPost(newPost: NewPostDisplay): NewPost {
    return {
      tab: newPost.tabs,
      title: newPost.title,
      tags: this.sanitizeTags(newPost.tags),
      content: newPost.content
    };
  }

  private sanitizeTags(tags: string): string[] {
    const splitTags = tags.split(",") || [];
    return splitTags.filter(tag => tag);
  }

}