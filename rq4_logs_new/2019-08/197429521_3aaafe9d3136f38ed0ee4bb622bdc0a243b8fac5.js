var $custodyHolder;
var $pickUpHolder;
var $alternateWeeksHolder;
var $partnerHolder;

jQuery(document).ready(function() {
    $custodyHolder = $('select#request_agreement_custody');

    $pickUpHolder = $('select#request_agreement_pick_up').parent().parent();
    $alternateWeeksHolder = $('input#request_agreement_alternate_weeks').parent().parent();
    $partnerHolder = $('select#request_agreement_partner').parent().parent();

    $pickUpHolder.hide();
    $alternateWeeksHolder.hide();
    $partnerHolder.hide();

    $custodyHolder.change(function(){
        var selectedCustody = $(this).children("option:selected").val();
        if(selectedCustody == 1){
            $pickUpHolder.show();
            $alternateWeeksHolder.show();
            $partnerHolder.hide();
        } else if(selectedCustody == 2){
            $pickUpHolder.hide();
            $alternateWeeksHolder.hide();
            $partnerHolder.show();
        } else{
            $pickUpHolder.hide();
            $alternateWeeksHolder.hide();
            $partnerHolder.hide();
        }
    });
});