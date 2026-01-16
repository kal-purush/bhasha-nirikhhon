  /*
  *  Additional jQuery code require for the Barcode Letter-Size plugin to run
  *
  *   - HP Gong 
  */
 
(function( $ ) {
// Display codetype option in div using show() and hide() function,
// and also display additional messages and max and min for the input boxes
$(document).ready(function(){
$("#codetype").change(function(){
$(this).find("option:selected").each(function(){
$('.message0').hide();
$('.message1').hide();
$('.message2').hide();
$('.message3').hide();
$('.message4').hide();
$('.message5').hide();
$('.message6').hide();
if($(this).attr("value")=="code128a"){
$(".box").not(".code128a").hide();
$(".code128a").show();
$('.message0').show();
$('.message1').show();
$("#pad1").attr({"max" : 11, "min" : 10 });
$("#pad2").attr({"max" : 30, "min": 24 }); 
}
else if($(this).attr("value")=="code128b"){
$(".box").not(".code128b").hide();
$(".code128b").show();
$('.message0').show();
$('.message2').show();
$("#pad1").attr({"max" : 11, "min" : 10 });
$("#pad2").attr({"max" : 30, "min": 24 }); 
}
else if($(this).attr("value")=="code128c"){
$(".box").not(".code128c").hide();
$(".code128c").show();
$('.message0').show();
$('.message3').show();
$("#pad1").attr({"max" : 11, "min" : 10 });
$("#pad2").attr({"max" : 30, "min": 24 }); 
}
else if($(this).attr("value")=="code39"){
$(".box").not(".code39").hide();
$(".code39").show();
$('.message0').show();
$('.message4').show();
$("#pad1").attr({"max" : 2, "min" : 1 });
$("#pad2").attr({"max" : 25, "min": 17 });
}
else if($(this).attr("value")=="code25"){
$(".box").not(".code25").hide();
$(".code25").show();
$('.message0').show();
$('.message5').show();
$("#pad1").attr({"max" : 2, "min" : 1 });
$("#pad2").attr({"max" : 25, "min": 17 });
}
else if($(this).attr("value")=="codabar"){
$(".box").not(".codabar").hide();
$(".codabar").show();
$('.message0').show();
$('.message6').show();
$("#pad1").attr({"max" : 2, "min" : 1 });
$("#pad2").attr({"max" : 25, "min": 17 });
}
else{
$(".box").hide();
}
});
}).change();
});

$(function(){
$('input[id=length]').keypress(function(event){
event.preventDefault();
});
$('input[id=padding1]').keypress(function(event){
   event.preventDefault();
});
$('input[id=padding2]').keypress(function(event){
   event.preventDefault();
});
});	

$(function(){
$('#printOut').click(function(e){
e.preventDefault();
var w = window.open();
var printOne = $('.print_barcodes').html();
w.document.write('<html><head><title>Generated Barcordes Lettersize</title></head><body><style type="text/css">@media print {@page { size:8.5in 11in; margin:.3in .1in .3in .1in; size: portrait; mso-header-margin:.3in; mso-footer-margin:.3in; mso-paper-source:0; orphans:10; widows:10;}</style><p style="-o-box-sizing: border-box; -webkit-box-sizing: border-box; -moz-box-sizing: border-box; box-sizing: border-box;margin: 0px 0px 0px 15px;">' + printOne + '</p></body></html>');
w.document.close();
return false;
});
});	

// This function will refresh the page back to the first page.
$(function(){	
$('#back').click(function() {
location.reload();
});	
});	
})( jQuery );