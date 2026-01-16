'use strict';

import {describe, beforeEachProviders, it, inject} from 'angular2/testing';

import {ContactService} from './contact.service';

describe('services', () => {

  beforeEachProviders(() => [
    ContactService
  ]);

  it('test one', inject([ContactService], (ContactService: any) => {
    console.log('services test...');
    console.log(ContactService.details());
  }));

});