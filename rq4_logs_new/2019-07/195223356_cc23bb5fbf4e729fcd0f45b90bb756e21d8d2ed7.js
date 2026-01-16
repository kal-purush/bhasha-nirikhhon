$(function(){
	$(".used-pc").change(function(){
		$("#left-pc").val(20 - $(this).val());
	});
	$(".pc-to-pa").change(function(){
		$("#left-pc").val(20 - $(this).val());
		$("#attribute-points").val(38 + 2 * $(this).val());
	});
});