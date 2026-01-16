import {it, inject, describe, beforeEachProviders} from '@angular/core/testing';
import {Crud} from './crud.component';

// Load the implementations that should be tested

describe('Crud', () => {
  // provide our implementations or mocks to the dependency injector
  beforeEachProviders(() => [
    Crud
  ]);

  it('should log ngOnInit', inject([Crud], (about) => {
    spyOn(console, 'log');
    expect(console.log).not.toHaveBeenCalled();

    about.ngOnInit();
    expect(console.log).toHaveBeenCalled();
  }));

});