import { RadicalTechArtPage } from './app.po';

describe('radical-tech-art App', () => {
  let page: RadicalTechArtPage;

  beforeEach(() => {
    page = new RadicalTechArtPage();
  });

  it('should display welcome message', () => {
    page.navigateTo();
    expect(page.getParagraphText()).toEqual('Welcome to app!!');
  });
});