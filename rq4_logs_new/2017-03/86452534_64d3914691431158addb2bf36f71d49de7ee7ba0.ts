import { AngularTCGPage } from './app.po';

describe('angular-tcg App', () => {
  let page: AngularTCGPage;

  beforeEach(() => {
    page = new AngularTCGPage();
  });

  it('should display message saying app works', () => {
    page.navigateTo();
    expect(page.getParagraphText()).toEqual('app works!');
  });
});