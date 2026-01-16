import { TripPlannerPage } from './app.po';

describe('trip-planner App', function() {
  let page: TripPlannerPage;

  beforeEach(() => {
    page = new TripPlannerPage();
  });

  it('should display message saying app works', () => {
    page.navigateTo();
    expect(page.getParagraphText()).toEqual('app works!');
  });
});