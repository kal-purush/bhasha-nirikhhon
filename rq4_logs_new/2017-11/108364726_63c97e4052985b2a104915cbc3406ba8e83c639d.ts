import { SpartaHackPage } from './app.po';

describe('sparta-hack App', () => {
  let page: SpartaHackPage;

  beforeEach(() => {
    page = new SpartaHackPage();
  });

  it('should display message saying app works', () => {
    page.navigateTo();
    expect(page.getParagraphText()).toEqual('app works!');
  });
});