import { BurlyPage } from './app.po';

describe('burly App', () => {
  let page: BurlyPage;

  beforeEach(() => {
    page = new BurlyPage();
  });

  it('should display message saying app works', () => {
    page.navigateTo();
    expect(page.getParagraphText()).toEqual('app works!');
  });
});