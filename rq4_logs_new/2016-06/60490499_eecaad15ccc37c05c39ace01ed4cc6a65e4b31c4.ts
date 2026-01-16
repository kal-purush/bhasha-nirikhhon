import {
  beforeEachProviders,
  describe,
  expect,
  it,
  inject
} from '@angular/core/testing';
import { CliPlanningPokerAppComponent } from '../app/cli-planning-poker.component';

beforeEachProviders(() => [CliPlanningPokerAppComponent]);

describe('App: CliPlanningPoker', () => {
  it('should create the app',
      inject([CliPlanningPokerAppComponent], (app: CliPlanningPokerAppComponent) => {
    expect(app).toBeTruthy();
  }));

  it('should have as title \'cli-planning-poker works!\'',
      inject([CliPlanningPokerAppComponent], (app: CliPlanningPokerAppComponent) => {
    expect(app.title).toEqual('cli-planning-poker works!');
  }));
});