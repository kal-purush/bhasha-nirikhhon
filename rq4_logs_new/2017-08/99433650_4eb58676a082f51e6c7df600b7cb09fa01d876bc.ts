import { ShoppingAngular2Page } from './app.po';

describe('shopping-angular2 App', () => {
  let page: ShoppingAngular2Page;

  beforeEach(() => {
    page = new ShoppingAngular2Page();
  });

  it('should display welcome message', () => {
    page.navigateTo();
    expect(page.getParagraphText()).toEqual('Welcome to app!');
  });
});