import { KottansGamePage } from './app.po';

describe('kottans-game App', () => {
  let page: KottansGamePage;

  beforeEach(() => {
    page = new KottansGamePage();
  });

  it('should display message saying app works', () => {
    page.navigateTo();
    expect(page.getParagraphText()).toEqual('app works!');
  });
});