import { PickadosAppPage } from './app.po';

describe('pickados-app App', () => {
  let page: PickadosAppPage;

  beforeEach(() => {
    page = new PickadosAppPage();
  });

  it('should display message saying app works', () => {
    page.navigateTo();
    expect(page.getParagraphText()).toEqual('app works!');
  });
});