import {describe, it, expect, beforeEachProviders, inject} from 'angular2/testing';
import {Angular2PlaygroundApp} from '../app/angular2-playground';

beforeEachProviders(() => [Angular2PlaygroundApp]);

describe('App: Angular2Playground', () => {
  it('should have the `defaultMeaning` as 42', inject([Angular2PlaygroundApp], (app) => {
    expect(app.defaultMeaning).toBe(42);
  }));

  describe('#meaningOfLife', () => {
    it('should get the meaning of life', inject([Angular2PlaygroundApp], (app) => {
      expect(app.meaningOfLife()).toBe('The meaning of life is 42');
      expect(app.meaningOfLife(22)).toBe('The meaning of life is 22');
    }));
  });
});
