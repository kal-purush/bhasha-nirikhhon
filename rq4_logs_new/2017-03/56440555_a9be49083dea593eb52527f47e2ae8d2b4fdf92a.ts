import { TcAppFrontendPage } from './app.po';

describe('tc-app-frontend App', () => {
  let page: TcAppFrontendPage;

  beforeEach(() => {
    page = new TcAppFrontendPage();
  });

  it('should display message saying app works', () => {
    page.navigateTo();
    expect(page.getParagraphText()).toEqual('app works!');
  });
});