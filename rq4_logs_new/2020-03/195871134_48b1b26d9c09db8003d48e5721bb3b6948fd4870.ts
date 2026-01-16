import { TestBed, inject } from '@angular/core/testing';

// import { TokenInterceptorService } from './token-interceptor.service';
// import { expect } from 'jasmine';
// import { it } from 'jasmine';
// import { beforeEach } from 'jasmine';
// import { describe } from 'jasmine';

describe('TokenInterceptorService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [TokenInterceptorService]
    });
  });

  it('should be created', inject([TokenInterceptorService], (service: TokenInterceptorService) => {
    expect(service).toBeTruthy();
  }));
});