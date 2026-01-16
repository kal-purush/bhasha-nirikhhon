import { DynamicAppPage } from './app.po';

describe('dynamic-app App', () => {
  let page: DynamicAppPage;

  beforeEach(() => {
    page = new DynamicAppPage();
  });

  it('should display message saying app works', () => {
    page.navigateTo();
    expect(page.getParagraphText()).toEqual('app works!');
  });
});