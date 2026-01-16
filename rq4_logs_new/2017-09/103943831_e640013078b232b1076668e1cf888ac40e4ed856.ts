import { LifetoutPage } from './app.po';

describe('lifetout App', () => {
  let page: LifetoutPage;

  beforeEach(() => {
    page = new LifetoutPage();
  });

  it('should display message saying app works', () => {
    page.navigateTo();
    expect(page.getParagraphText()).toEqual('app works!');
  });
});