import { AcrudRestPage } from './app.po';

describe('acrud-rest App', () => {
  let page: AcrudRestPage;

  beforeEach(() => {
    page = new AcrudRestPage();
  });

  it('should display message saying app works', () => {
    page.navigateTo();
    expect(page.getParagraphText()).toEqual('app works!');
  });
});