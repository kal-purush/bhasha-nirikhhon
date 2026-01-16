import { ComponentFixture, TestBed, async } from '@angular/core/testing';
import { Store } from '@ngrx/store';
import { of } from 'rxjs';
import { By } from '@angular/platform-browser';

import { MODULE_DECLARATIONS, MODULE_IMPORTS } from '../../../../../src/modules/content/content.module';
import { MockStoreModule, MockAction } from '../../../../utils/mock-store';
import { PostResponse } from '../../../../../src/modules/content/components/postsList/postsList.model';
import { PostsListService } from '../../../../../src/modules/content/components/postsList/postsList.service';
import { PostsListComponent } from '../../../../../src/modules/content/components/postsList/postsList.component';

const postsResponse: PostResponse = {
  meta: {total: 2, start: 0, end: 1},
  content: [
    {id: 1, createdAt: '123', title: 'test_title_1', tags: ['tag1'], content: 'test', commentsCount: 2},
    {id: 2, createdAt: '1234', title: 'test_title_2', tags: ['tag2', 'tag3'], content: 'test', commentsCount: 3}
  ]
};
const fakeState: any = {
  postsList: {
    posts: postsResponse.content
  },
  menu: {
    selectedTab: 'sample-tab'
  }
};

describe('PostsListComponent', () => {

  let store: any;
  let postsListService: jasmine.SpyObj<PostsListService>;
  
  let component: PostsListComponent;
  let fixture: ComponentFixture<PostsListComponent>;
  
  beforeEach(() => {

    postsListService = jasmine.createSpyObj('PostsListService', {
      getPosts: of(postsResponse)
    });

    TestBed.configureTestingModule({
      imports: [
        ...MODULE_IMPORTS,
        MockStoreModule.forRoot('postsList', fakeState.postsList),
        MockStoreModule.forRoot('menu', fakeState.menu)
      ],
      declarations: MODULE_DECLARATIONS,
      providers: [
        { provide: PostsListService, useValue: postsListService }
      ]
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(PostsListComponent);
    component = fixture.componentInstance;
    store = TestBed.get(Store);
    fixture.detectChanges();
  });

  it('should check if the component is defined', () => {
    expect(component).toBeDefined();
  });

  // @TODO Test posts HTML

  it('should call getPosts function on changing selected tab', () => {
    const tabName = 'some-other-tab';
    store.dispatch(new MockAction({selectedTab: tabName}));
    fixture.detectChanges();
    expect(postsListService.getPosts).toHaveBeenCalledWith(tabName);
    // @TODO Check if the state was filled up with new posts
  });

});