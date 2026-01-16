import { QbLibraryPage } from './app.po';

describe('qb-library App', function() {
  let page: QbLibraryPage;

  beforeEach(() => {
    page = new QbLibraryPage();
  });

  it('should display message saying app works', () => {
    page.navigateTo();
    expect(page.getParagraphText()).toEqual('app works!');
  });
});