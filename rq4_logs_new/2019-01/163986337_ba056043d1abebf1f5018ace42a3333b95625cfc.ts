import { ComponentFixture, TestBed } from '@angular/core/testing';
import { Store } from '@ngrx/store';
import { By } from '@angular/platform-browser';

import { MODULE_DECLARATIONS, MODULE_IMPORTS } from '../../../../../src/modules/content/content.module';
import { MockStoreModule, MockAction } from '../../../../utils/mock-store';
import { Post, PostResponse } from '../../../../../src/modules/content/components/postsList/postsList.model';
import { RecentPostsComponent, RECENT_POSTS_LIMIT } from '../../../../../src/modules/content/components/recentPosts/recentPosts.component';

const postsResponse: PostResponse = {
  meta: {total: 2, start: 0, end: 1},
  content: [
    {id: 1, createdAt: '123', title: 'test_title_1', tags: ['tag1'], content: 'test', commentsCount: 2},
    {id: 2, createdAt: '1234', title: 'test_title_2', tags: ['tag2', 'tag3'], content: 'test', commentsCount: 3},
    {id: 2, createdAt: '1234', title: 'test_title_2', tags: ['tag2', 'tag3'], content: 'test', commentsCount: 3},
    {id: 2, createdAt: '1234', title: 'test_title_2', tags: ['tag2', 'tag3'], content: 'test', commentsCount: 3},
    {id: 2, createdAt: '1234', title: 'test_title_2', tags: ['tag2', 'tag3'], content: 'test', commentsCount: 3}
  ]
};
const fakeState: any = {
  recentPosts: {
    recentPosts: null
  },
  postsList: {
    posts: postsResponse
  }
};

describe('PostsListComponent', () => {

  let store: any;
  
  let component: RecentPostsComponent;
  let fixture: ComponentFixture<RecentPostsComponent>;
  
  beforeEach(() => {

    TestBed.configureTestingModule({
      imports: [
        ...MODULE_IMPORTS,
        MockStoreModule.forRoot('recentPosts', fakeState.recentPosts),
        MockStoreModule.forRoot('postsList', fakeState.postsList)
      ],
      declarations: MODULE_DECLARATIONS,
      providers: []
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RecentPostsComponent);
    component = fixture.componentInstance;
    store = TestBed.get(Store);
    fixture.detectChanges();
  });

  it('should check if the component is defined', () => {
    expect(component).toBeDefined();
  });

  it('should have recent posts displayed', () => {
    const postWraps = fixture.debugElement.queryAll(By.css('.posts-wrap')).length;
    expect(postWraps).toEqual(RECENT_POSTS_LIMIT);
  });

});