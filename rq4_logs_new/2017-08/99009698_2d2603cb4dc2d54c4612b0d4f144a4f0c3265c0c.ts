import { Angular2HeroAppPage } from './app.po';

describe('angular2-hero-app App', () => {
  let page: Angular2HeroAppPage;

  beforeEach(() => {
    page = new Angular2HeroAppPage();
  });

  it('should display welcome message', () => {
    page.navigateTo();
    expect(page.getParagraphText()).toEqual('Welcome to app!');
  });
});