import { SuFirstExrPage } from './app.po';

describe('su-first-exr App', function() {
  let page: SuFirstExrPage;

  beforeEach(() => {
    page = new SuFirstExrPage();
  });

  it('should display message saying app works', () => {
    page.navigateTo();
    expect(page.getParagraphText()).toEqual('app works!');
  });
});