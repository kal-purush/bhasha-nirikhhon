describe('bio', function() {

  beforeEach(function() {
    browser.get('/dist/dev');
  });

  it('should have correct h2', function() {
      expect(element(by.css('app section bio h2')).getText())
      .toEqual('Bio');
  });

});