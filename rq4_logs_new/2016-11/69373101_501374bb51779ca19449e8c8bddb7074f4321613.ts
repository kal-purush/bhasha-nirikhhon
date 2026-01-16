import { NodejsTestProjectPage } from './app.po';

describe('nodejs-test-project App', function() {
  let page: NodejsTestProjectPage;

  beforeEach(() => {
    page = new NodejsTestProjectPage();
  });

  it('should display message saying app works', () => {
    page.navigateTo();
    expect(page.getParagraphText()).toEqual('app works!');
  });
});