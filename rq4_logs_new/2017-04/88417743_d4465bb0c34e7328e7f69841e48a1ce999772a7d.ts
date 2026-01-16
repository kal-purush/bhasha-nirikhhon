import { FankserverPage } from './app.po';

describe('fankserver App', () => {
  let page: FankserverPage;

  beforeEach(() => {
    page = new FankserverPage();
  });

  it('should display message saying app works', () => {
    page.navigateTo();
    expect(page.getParagraphText()).toEqual('fs works!');
  });
});