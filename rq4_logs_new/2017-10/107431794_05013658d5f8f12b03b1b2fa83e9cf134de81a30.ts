import { TestBed, inject } from '@angular/core/testing';

import { HofService } from './hof.service';

describe('HofService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [HofService]
    });
  });

  it('should be created', inject([HofService], (service: HofService) => {
    expect(service).toBeTruthy();
  }));
});