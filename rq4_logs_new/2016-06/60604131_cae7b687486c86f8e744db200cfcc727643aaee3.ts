import {
  beforeEachProviders,
  describe,
  expect,
  it,
  inject
} from '@angular/core/testing';
import { HomeAccountantAppComponent } from '../app/home-accountant.component';

beforeEachProviders(() => [HomeAccountantAppComponent]);

describe('App: HomeAccountant', () => {
  it('should create the app',
      inject([HomeAccountantAppComponent], (app: HomeAccountantAppComponent) => {
    expect(app).toBeTruthy();
  }));

  it('should have as title \'home-accountant works!\'',
      inject([HomeAccountantAppComponent], (app: HomeAccountantAppComponent) => {
    expect(app.title).toEqual('home-accountant works!');
  }));
});