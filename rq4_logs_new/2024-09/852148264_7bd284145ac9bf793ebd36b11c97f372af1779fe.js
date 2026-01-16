jQuery(document).ready(function ($) {
    $("md#output").hide();
    var mdVisible = false;
    $("#ShowMD").on('click', function(){
        var $this = $(this);
        $("md#output").slideToggle();
        mdVisible = !mdVisible;
        console.log(mdVisible);
        if(mdVisible){
            $this.find('span').text("Hide");
        }else{
            $this.find('span').text("Show");
        }
    })

});