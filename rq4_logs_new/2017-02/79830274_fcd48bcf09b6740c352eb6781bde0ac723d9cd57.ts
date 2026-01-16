import { Lesson7Page } from './app.po';

describe('lesson7 App', function() {
  let page: Lesson7Page;

  beforeEach(() => {
    page = new Lesson7Page();
  });

  it('should display message saying app works', () => {
    page.navigateTo();
    expect(page.getParagraphText()).toEqual('app works!');
  });
});