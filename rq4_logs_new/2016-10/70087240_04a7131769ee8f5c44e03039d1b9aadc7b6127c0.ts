import { AngularRegexPage } from './app.po';

describe('angular-regex App', function() {
  let page: AngularRegexPage;

  beforeEach(() => {
    page = new AngularRegexPage();
  });

  it('should display message saying app works', () => {
    page.navigateTo();
    expect(page.getParagraphText()).toEqual('app works!');
  });
});