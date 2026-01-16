import { TestBed } from '@angular/core/testing';

import { CurrentDateService } from './current-date.service';

describe('SelectDateService', () => {
  let service: CurrentDateService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(CurrentDateService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});