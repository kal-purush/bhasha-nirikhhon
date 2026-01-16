import { Angular2APPPage } from './app.po';

describe('angular2-app App', function() {
  let page: Angular2APPPage;

  beforeEach(() => {
    page = new Angular2APPPage();
  });

  it('should display message saying app works', () => {
    page.navigateTo();
    expect(page.getParagraphText()).toEqual('app works!');
  });
});