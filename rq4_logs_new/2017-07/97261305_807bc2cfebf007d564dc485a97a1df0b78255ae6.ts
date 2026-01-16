import { PwdlockPage } from './app.po';

describe('pwdlock App', () => {
  let page: PwdlockPage;

  beforeEach(() => {
    page = new PwdlockPage();
  });

  it('should display welcome message', () => {
    page.navigateTo();
    expect(page.getParagraphText()).toEqual('Welcome to app!!');
  });
});