describe('clicking on a button', function () {  
  // reset the counter before each test
  beforeEach(function() {
    $('#clear-button').click();
  })

  it('should be gray when submit is disabled', function () {
    // Get the color of submit button
    var color = $('#submit-button').css('backgroundColor');

    // hover the submit the button
    $('#submit-button').hover();

    // assert that it's grayed out'
    expect(color).toEqual("rgb(211, 211, 211)");
  });

  it('should be dark blue when submit is active', function () {
    // Get the color of submit button
    document.getElementById('submit-button').removeAttribute('disabled')
    var color = $('#submit-button').css('backgroundColor');

    // assert that it's dark blue'
    expect(color).toEqual("rgb(0, 45, 91)");
  });



  it('should be gray when clear is disabled', function () {
    // Get the color of clear button
    var color = $('#clear-button').css('backgroundColor');

    // hover the clear the button
    $('#clear-button').hover();

    // assert that it's grayed out'
    expect(color).toEqual("rgb(211, 211, 211)");
  });

  it('should be dark red  when clear is active', function () {
    // Get the color of clear button
    document.getElementById('clear-button').removeAttribute('disabled')
    var color = $('#clear-button').css('backgroundColor');

    // assert that it's dark red'
    expect(color).toEqual("rgb(194, 0, 23)");
  });


});
