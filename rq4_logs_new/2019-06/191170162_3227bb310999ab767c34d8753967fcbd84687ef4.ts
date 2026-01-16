import { TestBed } from '@angular/core/testing';

import { GetEmployeesService } from './get-employees.service';

describe('GetEmployeesService', () => {
  beforeEach(() => TestBed.configureTestingModule({}));

  it('should be created', () => {
    const service: GetEmployeesService = TestBed.get(GetEmployeesService);
    expect(service).toBeTruthy();
  });
});