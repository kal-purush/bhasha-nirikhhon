import { TestBed, inject } from '@angular/core/testing';

import { MatrixService } from './matrix.service';

describe('MatrixService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [MatrixService]
    });
  });

  it('should ...', inject([MatrixService], (service: MatrixService) => {
    expect(service).toBeTruthy();
  }));
});