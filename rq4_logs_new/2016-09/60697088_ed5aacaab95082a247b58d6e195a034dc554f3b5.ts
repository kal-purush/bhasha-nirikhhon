import { WemustflyNameNewPage } from './app.po';

describe('wemustfly-name-new App', function() {
  let page: WemustflyNameNewPage;

  beforeEach(() => {
    page = new WemustflyNameNewPage();
  });

  it('should display message saying app works', () => {
    page.navigateTo();
    expect(page.getParagraphText()).toEqual('app works!');
  });
});