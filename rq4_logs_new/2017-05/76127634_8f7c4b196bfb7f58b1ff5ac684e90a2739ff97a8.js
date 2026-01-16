// Add WorldPay redirection
$(document).bind('em_booking_gateway_add_banktransfer', function(event, response){
  // called by EM if return JSON contains gateway key, notifications messages are shown by now.
  if(response.result){
    var wpForm = $('<form action="'+response.banktransfer_url+'" method="post" id="em-banktransfer-redirect-form"></form>');
    $.each( response.banktransfer_vars, function(index,value){
      wpForm.append('<input type="hidden" name="'+index+'" value="'+value+'" />');
    });
    wpForm.append('<input id="em-banktransfer-submit" type="submit" style="display:none" />');
    wpForm.appendTo('body').trigger('submit');
  }
});