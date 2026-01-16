import { TestBed } from '@angular/core/testing';
import { CanActivateFn } from '@angular/router';

import { categoryGuard } from './category.guard';

describe('categoryGuard', () => {
  const executeGuard: CanActivateFn = (...guardParameters) => 
      TestBed.runInInjectionContext(() => categoryGuard(...guardParameters));

  beforeEach(() => {
    TestBed.configureTestingModule({});
  });

  it('should be created', () => {
    expect(executeGuard).toBeTruthy();
  });
});