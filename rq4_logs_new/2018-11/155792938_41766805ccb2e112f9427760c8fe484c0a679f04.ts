import { TestBed } from '@angular/core/testing';

import { HitApiService } from './hit-api.service';

describe('HitApiService', () => {
  beforeEach(() => TestBed.configureTestingModule({}));

  it('should be created', () => {
    const service: HitApiService = TestBed.get(HitApiService);
    expect(service).toBeTruthy();
  });
});