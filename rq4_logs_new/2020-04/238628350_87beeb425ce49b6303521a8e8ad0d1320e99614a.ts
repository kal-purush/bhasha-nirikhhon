import { TestBed } from '@angular/core/testing';

import { MealPlannerService } from './meal-planner.service';

describe('MealPlannerService', () => {
  beforeEach(() => TestBed.configureTestingModule({}));

  it('should be created', () => {
    const service: MealPlannerService = TestBed.get(MealPlannerService);
    expect(service).toBeTruthy();
  });
});