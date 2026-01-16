import { TestBed, inject } from '@angular/core/testing';

import { EmployServiceService } from './employ-service.service';

describe('EmployServiceService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [EmployServiceService]
    });
  });

  it('should be created', inject([EmployServiceService], (service: EmployServiceService) => {
    expect(service).toBeTruthy();
  }));
});