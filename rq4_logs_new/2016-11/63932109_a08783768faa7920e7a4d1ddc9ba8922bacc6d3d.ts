import { Ng2AmapInputPage } from './app.po';

describe('ec-front App', function() {
  let page: Ng2AmapInputPage;

  beforeEach(() => {
    page = new Ng2AmapInputPage();
  });

  it('should display message saying app works', () => {
    page.navigateTo();
    expect(page.getParagraphText()).toEqual('app works!');
  });
});