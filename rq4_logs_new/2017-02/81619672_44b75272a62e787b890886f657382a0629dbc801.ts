import { BankApplicationPage } from './app.po';

describe('bank-application App', function() {
  let page: BankApplicationPage;

  beforeEach(() => {
    page = new BankApplicationPage();
  });

  it('should display message saying app works', () => {
    page.navigateTo();
    expect(page.getParagraphText()).toEqual('app works!');
  });
});