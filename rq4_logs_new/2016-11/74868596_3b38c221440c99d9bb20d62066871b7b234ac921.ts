import { ZenithClientSitePage } from './app.po';

describe('zenith-client-site App', function() {
  let page: ZenithClientSitePage;

  beforeEach(() => {
    page = new ZenithClientSitePage();
  });

  it('should display message saying app works', () => {
    page.navigateTo();
    expect(page.getParagraphText()).toEqual('app works!');
  });
});