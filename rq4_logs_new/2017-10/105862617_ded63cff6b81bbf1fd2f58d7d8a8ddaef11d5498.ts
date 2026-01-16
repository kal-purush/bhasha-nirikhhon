import { BasePage } from './app.po';

describe('base App', () => {
  let page: BasePage;

  beforeEach(() => {
    page = new BasePage();
  });

  it('should display welcome message', () => {
    page.navigateTo();
    expect(page.getParagraphText()).toEqual('Welcome to app!!');
  });
});