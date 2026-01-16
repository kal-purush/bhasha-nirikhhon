import { TestBed, inject } from '@angular/core/testing';

import { WakanderTribeService } from './wakander-tribe.service';

describe('WakanderTribeService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [WakanderTribeService]
    });
  });

  it('should be created', inject([WakanderTribeService], (service: WakanderTribeService) => {
    expect(service).toBeTruthy();
  }));
});