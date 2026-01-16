import { TestBed, inject } from '@angular/core/testing';

import { GetproductsService } from './getproducts.service';

describe('GetproductsService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [GetproductsService]
    });
  });

  it('should be created', inject([GetproductsService], (service: GetproductsService) => {
    expect(service).toBeTruthy();
  }));
});