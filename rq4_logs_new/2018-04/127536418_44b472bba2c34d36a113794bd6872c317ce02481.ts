import { TestBed, inject } from '@angular/core/testing';

import { TenthQueryService } from './tenth-query.service';

describe('TenthQueryService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [TenthQueryService]
    });
  });

  it('should be created', inject([TenthQueryService], (service: TenthQueryService) => {
    expect(service).toBeTruthy();
  }));
});