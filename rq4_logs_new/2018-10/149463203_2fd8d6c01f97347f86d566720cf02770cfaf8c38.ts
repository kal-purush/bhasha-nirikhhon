import { TestBed } from '@angular/core/testing';

import { MovieListsService } from './movie-lists.service';

describe('MovieListsService', () => {
  beforeEach(() => TestBed.configureTestingModule({}));

  it('should be created', () => {
    const service: MovieListsService = TestBed.get(MovieListsService);
    expect(service).toBeTruthy();
  });
});