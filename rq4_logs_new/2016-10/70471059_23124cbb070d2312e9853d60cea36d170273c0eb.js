Stripe.setPublishableKey('pk_test_lxfWCc5be2HsI3rGx7o60Qvd');
var stripeResponseHandler = function(status, response) {
  var $Form = (jQuery)('#stripe-payment');
  if (response.error) {
    // Show the errors on the form
    $Form.find('.payment-errors').text(response.error.message);
    $Form.find('button').prop('disabled', false);
  } else {
    // token contains id, last4, and card type
    var Token = response.id;
    // Insert the token into the form so it gets submitted to the server
    //$Form.append((jQuery)('<input type="hidden" name="stripeToken" />').val(Token));
    $Form.find('#stripeToken').val(Token);
    // and re-submit
    $Form.get(0).submit();
  }
};
jQuery(function($) {

  $('#stripe-payment').submit(function(e) {
    var $Form = $(this);
    // Disable the submit button to prevent repeated clicks
    $Form.find('button').prop('disabled', true);
    Stripe.card.createToken($Form, stripeResponseHandler);
    // Prevent the form from submitting with the default action
    return false;
  });
});