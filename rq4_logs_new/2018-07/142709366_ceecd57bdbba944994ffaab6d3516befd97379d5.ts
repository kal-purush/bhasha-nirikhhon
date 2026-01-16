import { TestBed, inject } from '@angular/core/testing';

import { BertoniConfigService } from './bertoni-config.service';

describe('BertoniConfigService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [BertoniConfigService]
    });
  });

  it('should be created', inject([BertoniConfigService], (service: BertoniConfigService) => {
    expect(service).toBeTruthy();
  }));
});