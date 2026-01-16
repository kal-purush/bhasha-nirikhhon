import { TestBed } from '@angular/core/testing';

import { RecentPostsService } from '../../../../../src/modules/content/components/recentPosts/recentPosts.service';
import { SharedStubs } from '../../../../utils/stubs/sharedStubs';
import { ApiClient } from '../../../../../src/shared/clients/api.client';

describe('RecentPostsService', () => {

  let recentPostsService: RecentPostsService;
  let apiClient: jasmine.SpyObj<ApiClient>;

  beforeEach(() => {

    apiClient = SharedStubs.getApiClientStub();

    TestBed.configureTestingModule({
      providers: [
        RecentPostsService,
        { provide: ApiClient, useValue: apiClient }
      ]
    });

  });

  beforeEach(() => {
    recentPostsService = TestBed.get(RecentPostsService);
  });

  it(`
    WHEN there is a call to getRecentPosts method
    THEN ApiClient.getRecentPosts method should be called
  `, () => {
    recentPostsService.getRecentPosts();
    expect(apiClient.getRecentPosts).toHaveBeenCalled();
  });

});