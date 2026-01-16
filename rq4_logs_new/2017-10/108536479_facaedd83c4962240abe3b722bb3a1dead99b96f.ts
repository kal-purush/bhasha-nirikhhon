import { AngularCodePage } from './app.po';

describe('angular-code App', () => {
  let page: AngularCodePage;

  beforeEach(() => {
    page = new AngularCodePage();
  });

  it('should display welcome message', () => {
    page.navigateTo();
    expect(page.getParagraphText()).toEqual('Welcome to app!');
  });
});