describe('projects', function() {

  beforeEach(function() {
    browser.get('/dist/dev');
  });

  it('should have correct h1', function() {
      expect(element(by.css('app section projects h1')).getText())
      .toEqual('Projects');
  });

  it('should have correct h2', function() {
      expect(element(by.css('app section projects h2')).getText())
      .toEqual('My Projects');
  });

  // it('should have correct success msg', function() {
  //     expect(element(by.css('app section projects p')).getText())
  //     .toEqual('Here is some text');
  // });
});