/* tslint:disable:no-unused-variable */
import { async, ComponentFixture, TestBed, inject } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import { DebugElement } from '@angular/core';

import { Radio, Id } from './radio';

describe('Radio Service', () => {
  let service: Radio;

  beforeEach(async(() => {
    service = new Radio();
  }));

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [Radio]
    });
  });

  it('should ...', inject([Radio], (service: Radio) => {
    expect(service).toBeTruthy();
  }));

  it('should create an empty stores', () => {
    expect(service.stores).toEqual({});
  });

  it('should create', () => {
    let value = '';
    const next = data => value = data;
    const destroy = service.subscribe(Id, next);
    service.dispatch(Id, ['message']);

    expect(service.stores[Id]).toEqual(jasmine.any(Array));
    expect(service.stores[Id].length).toEqual(1);
    expect(value).toEqual(['message']);

    destroy();
    expect(service.stores[Id].length).toEqual(0);

    value = 'freeze';
    service.dispatch(Id, ['message']);

    expect(value).toEqual('freeze');
  });
});
