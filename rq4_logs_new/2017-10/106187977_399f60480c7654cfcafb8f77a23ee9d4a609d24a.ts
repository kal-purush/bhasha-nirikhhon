import { TestBed, inject } from '@angular/core/testing';

import { SignupApiService } from './signup-api.service';

describe('SignupApiService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [SignupApiService],
    });
  });

  it('should be created', inject([SignupApiService], (service: SignupApiService) => {
    expect(service).toBeTruthy();
  }));
});