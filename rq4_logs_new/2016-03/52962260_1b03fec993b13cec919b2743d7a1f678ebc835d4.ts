import {describe, it, expect, beforeEachProviders, inject} from 'angular2/testing';
import {AnotherAppApp} from '../app/another-app';

beforeEachProviders(() => [AnotherAppApp]);

describe('App: AnotherApp', () => {
  it('should have the `defaultMeaning` as 42', inject([AnotherAppApp], (app: AnotherAppApp) => {
    expect(app.defaultMeaning).toBe(42);
  }));

  describe('#meaningOfLife', () => {
    it('should get the meaning of life', inject([AnotherAppApp], (app: AnotherAppApp) => {
      expect(app.meaningOfLife()).toBe('The meaning of life is 42');
      expect(app.meaningOfLife(22)).toBe('The meaning of life is 22');
    }));
  });
});
