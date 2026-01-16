import { DHUDClientPage } from './app.po';

describe('dhud-client App', () => {
  let page: DHUDClientPage;

  beforeEach(() => {
    page = new DHUDClientPage();
  });

  it('should display welcome message', () => {
    page.navigateTo();
    expect(page.getParagraphText()).toEqual('Welcome to app!');
  });
});