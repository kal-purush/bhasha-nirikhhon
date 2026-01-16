import { map, mergeMap } from 'rxjs/operators'
import { ofType } from 'redux-observable'
import { ajax } from 'rxjs/ajax';

import { LOAD_BLOG_POSTS, blogPostsLoaded } from '../modules/blog'

const blogPostEpic = action$ => action$.pipe(
  ofType(LOAD_BLOG_POSTS),
  mergeMap(action =>
    ajax.getJSON('http://localhost:8000/blog/posts').pipe(
      map(blogPosts => blogPostsLoaded(blogPosts))
    )
  ),
);

export default blogPostEpic