import { Angular2QuickstartPage } from './app.po';

describe('angular2-quickstart App', function() {
  let page: Angular2QuickstartPage;

  beforeEach(() => {
    page = new Angular2QuickstartPage();
  });

  it('should display message saying app works', () => {
    page.navigateTo();
    expect(page.getParagraphText()).toEqual('app works!');
  });
});