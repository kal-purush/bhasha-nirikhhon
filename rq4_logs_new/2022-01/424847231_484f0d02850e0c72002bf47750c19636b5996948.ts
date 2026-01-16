import { TestBed } from '@angular/core/testing';

import { ParseServicesService } from './parse-services.service';

describe('ParseServicesService', () => {
  let service: ParseServicesService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(ParseServicesService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});