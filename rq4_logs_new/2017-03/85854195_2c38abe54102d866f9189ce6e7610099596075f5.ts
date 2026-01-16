import { GsxrPage } from './app.po';

describe('gsxr App', () => {
  let page: GsxrPage;

  beforeEach(() => {
    page = new GsxrPage();
  });

  it('should display message saying app works', () => {
    page.navigateTo();
    expect(page.getParagraphText()).toEqual('app works!');
  });
});