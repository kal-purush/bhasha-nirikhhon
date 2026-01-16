/* tslint:disable:no-unused-variable */

import {
  beforeEach, beforeEachProviders,
  describe, xdescribe,
  expect, it, xit,
  async, inject
} from '@angular/core/testing';
import { ExampleService } from './example.service';

describe('Example Service', () => {
  beforeEachProviders(() => [ExampleService]);

  it('should ...',
      inject([ExampleService], (service: ExampleService) => {
    expect(service).toBeTruthy();
  }));
});