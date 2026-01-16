import { TestBed } from '@angular/core/testing';

import { OwnerRestaurantsService } from './owner-restaurants.service';

describe('OwnerRestaurantsService', () => {
  let service: OwnerRestaurantsService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(OwnerRestaurantsService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});