import { TestBed, inject } from '@angular/core/testing';

import { AutocompletionService } from './autocompletion.service';

describe('AutocompletionService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [AutocompletionService]
    });
  });

  it('should be created', inject([AutocompletionService], (service: AutocompletionService) => {
    expect(service).toBeTruthy();
  }));
});