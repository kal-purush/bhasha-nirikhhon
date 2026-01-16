import { SdashFrontPage } from './app.po';

describe('sdash-front App', function() {
  let page: SdashFrontPage;

  beforeEach(() => {
    page = new SdashFrontPage();
  });

  it('should display message saying app works', () => {
    page.navigateTo();
    expect(page.getParagraphText()).toEqual('app works!');
  });
});