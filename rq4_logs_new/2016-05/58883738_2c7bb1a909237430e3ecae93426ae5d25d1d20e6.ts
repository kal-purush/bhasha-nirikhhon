import {
  beforeEachProviders,
  describe,
  expect,
  it,
  inject
} from '@angular/core/testing';
import { Recipeshopper21AppComponent } from '../app/recipeshopper21.component';

beforeEachProviders(() => [Recipeshopper21AppComponent]);

describe('App: Recipeshopper21', () => {
  it('should create the app',
      inject([Recipeshopper21AppComponent], (app: Recipeshopper21AppComponent) => {
    expect(app).toBeTruthy();
  }));

  it('should have as title \'recipeshopper21 works!\'',
      inject([Recipeshopper21AppComponent], (app: Recipeshopper21AppComponent) => {
    expect(app.title).toEqual('recipeshopper21 works!');
  }));
});