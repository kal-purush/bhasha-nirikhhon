/* tslint:disable:no-unused-variable */

import { TestBed, async, inject } from '@angular/core/testing';
import { FormsService } from './forms.service';

describe('Service: Forms', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [FormsService]
    });
  });

  it('should ...', inject([FormsService], (service: FormsService) => {
    expect(service).toBeTruthy();
  }));
});