import { AotReproPage } from './app.po';

describe('aot-repro App', function() {
  let page: AotReproPage;

  beforeEach(() => {
    page = new AotReproPage();
  });

  it('should display message saying app works', () => {
    page.navigateTo();
    expect(page.getParagraphText()).toEqual('app works!');
  });
});