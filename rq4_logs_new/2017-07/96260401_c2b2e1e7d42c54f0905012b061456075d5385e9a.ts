import { BrAppPage } from './app.po';

describe('br-app App', () => {
  let page: BrAppPage;

  beforeEach(() => {
    page = new BrAppPage();
  });

  it('should display welcome message', () => {
    page.navigateTo();
    expect(page.getParagraphText()).toEqual('Welcome to app!!');
  });
});