import { Ref } from '@w5s/core';
import { describe, it, expect } from 'vitest';
import { ApplicationTest } from './application.js';

describe('ApplicationTest', () => {
  it('should return new application', () => {
    const store = Ref({});
    const id = 'test-app';
    expect(ApplicationTest({ id, store, foo: 1, bar: 2 })).toEqual({
      id,
      initialConfiguration: {
        foo: 1,
        bar: 2,
      },
      current: {
        configuration: {
          foo: 1,
          bar: 2,
        },
      },
      store,
    });
  });
  it('should return a reference to store', () => {
    const store = Ref({});
    const id = 'test-app';
    const app = ApplicationTest({ id, store, foo: 1, bar: 2 });
    expect(app.store.current).toEqual({
      'test-app': {
        configuration: {
          bar: 2,
          foo: 1,
        },
      },
    });
  });
});