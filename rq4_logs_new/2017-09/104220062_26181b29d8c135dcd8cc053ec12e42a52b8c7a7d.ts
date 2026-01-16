import { JUberPage } from './app.po';

describe('j-uber App', function() {
  let page: JUberPage;

  beforeEach(() => {
    page = new JUberPage();
  });

  it('should display message saying app works', () => {
    page.navigateTo();
    expect(page.getParagraphText()).toEqual('app works!');
  });
});