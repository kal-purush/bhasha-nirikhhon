import { ItunesStorePage } from './app.po';

describe('itunes-store App', () => {
  let page: ItunesStorePage;

  beforeEach(() => {
    page = new ItunesStorePage();
  });

  it('should display welcome message', () => {
    page.navigateTo();
    expect(page.getParagraphText()).toEqual('Welcome to app!!');
  });
});