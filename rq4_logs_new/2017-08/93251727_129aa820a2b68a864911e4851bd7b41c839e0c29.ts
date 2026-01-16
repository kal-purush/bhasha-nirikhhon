import { HeroesCliPage } from './app.po';

describe('andel App', () => {
  let page: HeroesCliPage;

  beforeEach(() => {
    page = new HeroesCliPage();
  });

  it('should display welcome message', () => {
    page.navigateTo();
    expect(page.getParagraphText()).toEqual('Tour of heroes');
  });
});