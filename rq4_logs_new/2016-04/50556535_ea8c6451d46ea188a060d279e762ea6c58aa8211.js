describe('clinical:csv', function () {
  var server = meteor();
  var client = browser(server);

  it('Should exist on the client', function () {
    return client.execute(function () {
      expect($('body').css('background-size')).to.equal('cover');
      expect($('body').css('-webkit-user-select')).to.equal('none');
    });
  });

});