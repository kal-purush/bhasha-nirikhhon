import { TestBed, inject } from '@angular/core/testing';

import { PersonasService } from './personas.service';

describe('PersonasService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [PersonasService]
    });
  });

  it('should ...', inject([PersonasService], (service: PersonasService) => {
    expect(service).toBeTruthy();
  }));
});