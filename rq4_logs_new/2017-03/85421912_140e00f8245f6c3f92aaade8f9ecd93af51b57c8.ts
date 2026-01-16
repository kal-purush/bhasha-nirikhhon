import { AngularPart2Page } from './app.po';

describe('angular-part2 App', function() {
  let page: AngularPart2Page;

  beforeEach(() => {
    page = new AngularPart2Page();
  });

  it('should display message saying app works', () => {
    page.navigateTo();
    expect(page.getParagraphText()).toEqual('app works!');
  });
});