$(document).ready(function(){
	// $('#set').click(function(){
	// 	$('#setter').val("hello world!"); // Установка значения
	// });
		$('#get').click(function(){
			var alt = parseInt(($('#alt').val())) / 11 || 0;
			// var alt = parseInt(($('#alt').val())) / 11;
			var chrom = parseInt(($('#chrom').val())) / 11 || 0;
			var alchemy = parseInt(($('#alchemy').val())) / 3 || 0;
			var chaos = parseInt(($('#chaos').val())) / 1 || 0;
			var chance = parseInt(($('#chance').val())) / 6 || 0;
			var chisel = parseInt(($('#chisel').val())) / 2.7 || 0;
			var gcp = parseInt(($('#gcp').val())) / 1 || 0;
			var vaal = parseInt(($('#vaal').val())) / 2.2 || 0;
			var scouring = parseInt(($('#scouring').val())) / 1.5 || 0;
			var regret = parseInt(($('#regret').val())) / 1 || 0;
			var jewellery = parseInt(($('#jewellery').val()))  / 12 || 0;
			var fusing = parseInt(($('#fusing').val())) / 3 || 0;
			var regal = parseInt(($('#regal').val())) / 1 || 0;
			var divine = parseInt(($('#divine').val())) * 11 || 0;
			var exalted = parseInt(($('#exalted').val())) * 42 || 0;
			var pushUps = ((alt + chrom + alchemy + chaos + chance + chisel + gcp +
				vaal + scouring + regret + jewellery + fusing + regal + divine + exalted) * 2);

			if (Math.round(pushUps * 10) / 10 >= 7) {

				alert(Math.ceil(pushUps));
			}else{
				alert(Math.floor(pushUps));
			}

	});

});