import { TestBed, inject } from '@angular/core/testing';

import { GmapSAService } from './gmap.service';

describe('GmapSAService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [GmapSAService]
    });
  });

  it('should be created', inject([GmapSAService], (service: GmapSAService) => {
    expect(service).toBeTruthy();
  }));
});