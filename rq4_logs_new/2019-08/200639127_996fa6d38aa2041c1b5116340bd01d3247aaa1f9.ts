import { TestBed } from '@angular/core/testing';

import { ExpenseManagerService } from './expense-manager.service';

describe('ExpenseManagerService', () => {
  beforeEach(() => TestBed.configureTestingModule({}));

  it('should be created', () => {
    const service: ExpenseManagerService = TestBed.get(ExpenseManagerService);
    expect(service).toBeTruthy();
  });
});