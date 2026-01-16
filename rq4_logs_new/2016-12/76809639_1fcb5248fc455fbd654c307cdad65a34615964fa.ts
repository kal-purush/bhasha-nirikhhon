import { Angular2ClarityPage } from './app.po';

describe('angular2-clarity App', function() {
  let page: Angular2ClarityPage;

  beforeEach(() => {
    page = new Angular2ClarityPage();
  });

  it('should display message saying app works', () => {
    page.navigateTo();
    expect(page.getParagraphText()).toEqual('app works!');
  });
});