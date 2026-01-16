$(document).ready(function(){
	chk();
	
	$("#codLi").hide();
	
	function chk(){
		var provinsiId = $("#provinsiList").val();
		var key = $("#kabId").val();
		
		$.post("json-region", { provinsiId : provinsiId}, function(data) {
			var value = data.jsonResult.kabupatenList;
			$("#kabupatenList").empty();
			
			for(var i=0 ; i<value.length ; i++){
				var model = value[i];
				$option=$("<option/>");
				$option.attr("value",model[0]);
				$option.text(model[1]);
				if(key == model[0]){
					$option.attr("selected", true);
				}
				$("#kabupatenList").append($option);
			}
		});   
	}
	
	function commaSeparateNumber(val){
	    while (/(\d+)(\d{3})/.test(val.toString())){
	      val = val.toString().replace(/(\d+)(\d{3})/, '$1'+','+'$2');
	    }
	    return val;
	  }
	
	chkTotal();
	
	$("#shipTypeList").change(function(){
		chkTotal();
	});
	
	function chkTotal(){
		var subtotal = $("#sub").text();
		var shipTypeId = $("#shipTypeList").val();
		var totalBottle = $("#totalBottle").text();
		var totalFee = "";

		if(shipTypeId == 1){
			if(totalBottle > 12){
				totalFee = '150,000';
			} else if(totalBottle > 6){
				totalFee = '100,000';
			} else if(totalBottle > 3){
				totalFee = '70,000';
			} else {
				totalFee = '50,000';
			}
			
		} else if(shipTypeId == 2){
			if(totalBottle > 12){
				totalFee = '200,000';
			} else if(totalBottle > 6){
				totalFee = '150,000';
			} else if(totalBottle > 3){
				totalFee = '120,000';
			} else {
				totalFee = '100,000';
			}
		} else if(shipTypeId == 3){
			if(totalBottle > 12){
				totalFee = '170,000';
			} else if(totalBottle > 6){
				totalFee = '140,000';
			} else if(totalBottle > 3){
				totalFee = '90,000';
			} else {
				totalFee = '60,000';
			}
			
		} else if(shipTypeId == 4){
			if(totalBottle > 12){
				totalFee = '180,000';
			} else if(totalBottle > 6){
				totalFee = '150,000';
			} else if(totalBottle > 3){
				totalFee = '100,000';
			} else {
				totalFee = '70,000';
			}
		}
		
		$("#ship").text(totalFee);
		var total = parseInt(subtotal.replace(/,/g, '')) + parseInt(totalFee.replace(/,/g, ''));
		
		if($("#cod").prop('checked') == true){
			total = total + 10000;
			$("#admin").show();
		} else {
			$("#admin").hide();
		}
		
		$("#total").text(commaSeparateNumber(total));
		
		//update hidden shipping_form & total_form
		$("#shipping_form").val(totalFee);
		$("#total_form").val(total);
		
		var exchangeValue = $("#exchangeValue").val();
		var usd = ((total/exchangeValue)*1.039)+0.3;
		
		$("#paypal_total").val(Math.ceil(usd));
		$("#paypal").text("$"+Math.ceil(usd)+" USD");
		
		chkRadio();
	}
	
	
	$(".input-radio").click(function(){
		chkTotal();
	});
	
	
	$("#chkbox").click(function(){
		if(this.checked == true){
			chkProv1();
			chkKab1();
			$("#shipAddr").hide();
			$("#shipAddr").find('input').each(function(){
				$(this).removeAttr('required');
			});
		} else {
			chkProv2();
			chkKab2();
			$("#shipAddr").show();
			$("#shipAddr").find('input').each(function(){
				$(this).attr('required', 'true');
			});
		}

		chkTotal();
	});
	
	function chkRadio(){
		if($("#cc").prop('checked') == true){
			$(".paypal").each(function(){
				$(this).show();
			});
		} else {
			$(".paypal").each(function(){
				$(this).hide();
			});
		}
	}
	
	
	function chkProv1(){
		var provinsiId = $("#provinsiList").val();
		if(document.getElementById('chkbox').checked == true){
			if(provinsiId == 11 || provinsiId == 12 || provinsiId == 13 || provinsiId == 14 || provinsiId == 15 ){
				$("#shipTypeList").empty();

				$option=$("<option/>");
				$option.attr("value",3);
				$option.text("JNE Express (Dalam Jawa)");
				$("#shipTypeList").append($option);
				$("#bca").prop('checked', true);
				$("#codLi").hide();
			} else {
				$("#shipTypeList").empty();
				
				$option=$("<option/>");
				$option.attr("value",4);
				$option.text("JNE Express (Luar Jawa)");
				$("#shipTypeList").append($option);
				$("#bca").prop('checked', true);
				$("#codLi").hide();
			}
		}
	}
	
	function chkKab1(){
		var kabupatenId = $("#kabupatenList").val();
		var provinsiId = $("#provinsiList").val();
		
		if(document.getElementById('chkbox').checked == true){
			if(kabupatenId == 266 || kabupatenId == 269 || kabupatenId == 153 || kabupatenId == 154 || kabupatenId == 155 || kabupatenId == 156 || kabupatenId == 157 || kabupatenId == 181 || kabupatenId == 161 || kabupatenId == 178 || kabupatenId == 262 || kabupatenId == 266 || kabupatenId == 269 || kabupatenId == 160 || kabupatenId == 177){
				$("#shipTypeList").empty();
				
				$option=$("<option/>");
				$option.attr("value",1);
				$option.text("Jabodetabek Standard");
				$("#shipTypeList").append($option);
				
				$option=$("<option/>");
				$option.attr("value",2);
				$option.text("Jabodetabek Priority");
				$("#shipTypeList").append($option);
				
				$("#codLi").show();
				
			} else if(provinsiId == 11 || provinsiId == 12 || provinsiId == 13 || provinsiId == 14 || provinsiId == 15 ){
				$("#shipTypeList").empty();
				
				$option=$("<option/>");
				$option.attr("value",3);
				$option.text("JNE Express (Dalam Jawa)");
				$("#shipTypeList").append($option);
				$("#bca").prop('checked', true);
				$("#codLi").hide();
			} else {
				$("#shipTypeList").empty();
				
				$option=$("<option/>");
				$option.attr("value",4);
				$option.text("JNE Express (Luar Jawa)");
				$("#shipTypeList").append($option);
				$("#bca").prop('checked', true);
				$("#codLi").hide();
			}
		}
	}
	
	
	$("#provinsiList").change(function(){
		var provinsiId = $(this).val();
		chkProv1();
		
		$.post("json-region", { provinsiId : provinsiId}, function(data) {
			var value = data.jsonResult.kabupatenList;
			$("#kabupatenList").empty();
			
			for(var i=0 ; i<value.length ; i++){
				$option=$("<option/>");
				$option.attr("value",value[i][0]);
				$option.text(value[i][1]);
				$("#kabupatenList").append($option);
			}
		});   
		chkTotal();
	});
	
	$("#kabupatenList").change(function(){
		chkKab1();
		chkTotal();
	});
	
	
	
	//list2
	function chkProv2(){
		var provinsiId = $("#provinsiList2").val();
		if(document.getElementById('chkbox').checked != true){
			if(provinsiId == 11 || provinsiId == 12 || provinsiId == 13 || provinsiId == 14 || provinsiId == 15 ){
				$("#shipTypeList").empty();
				
				$option=$("<option/>");
				$option.attr("value",3);
				$option.text("JNE Express (Dalam Jawa)");
				$("#shipTypeList").append($option);
				$("#bca").prop('checked', true);
				$("#codLi").hide();
			} else {
				$("#shipTypeList").empty();
				
				$option=$("<option/>");
				$option.attr("value",4);
				$option.text("JNE Express (Luar Jawa)");
				$("#shipTypeList").append($option);
				$("#bca").prop('checked', true);
				$("#codLi").hide();
			}
		}
	}
	
	function chkKab2(){
		var kabupatenId = $("#kabupatenList2").val();
		var provinsiId = $("#provinsiList2").val();
		
		if(document.getElementById('chkbox').checked != true){
			if(kabupatenId == 153 || kabupatenId == 154 || kabupatenId == 155 || kabupatenId == 156 || kabupatenId == 157 || kabupatenId == 181 || kabupatenId == 161 || kabupatenId == 178 || kabupatenId == 262 || kabupatenId == 266 || kabupatenId == 269 || kabupatenId == 160 || kabupatenId == 177){
				$("#shipTypeList").empty();
				
				$option=$("<option/>");
				$option.attr("value",1);
				$option.text("Jabodetabek Standard");
				$("#shipTypeList").append($option);
				
				$option=$("<option/>");
				$option.attr("value",2);
				$option.text("Jabodetabek Priority");
				$("#shipTypeList").append($option);

				$("#codLi").show();
			} else if(provinsiId == 11 || provinsiId == 12 || provinsiId == 13 || provinsiId == 14 || provinsiId == 15 ){
				$("#shipTypeList").empty();
				
				$option=$("<option/>");
				$option.attr("value",3);
				$option.text("JNE Express (Dalam Jawa)");
				$("#shipTypeList").append($option);
				$("#bca").prop('checked', true);
				$("#codLi").hide();
			} else {
				$("#shipTypeList").empty();
				
				$option=$("<option/>");
				$option.attr("value",4);
				$option.text("JNE Express (Luar Jawa)");
				$("#shipTypeList").append($option);
				$("#bca").prop('checked', true);
				$("#codLi").hide();
			}
		}
	}
	
	
	$("#provinsiList2").change(function(){
		var provinsiId = $(this).val();
		
		chkProv2();
		
		$.post("json-region", { provinsiId : provinsiId}, function(data) {
			var value = data.jsonResult.kabupatenList;
			$("#kabupatenList2").empty();
			
			for(var i=0 ; i<value.length ; i++){
				$option=$("<option/>");
				$option.attr("value",value[i][0]);
				$option.text(value[i][1]);
				$("#kabupatenList2").append($option);
			}
		});   
		chkTotal();
	});
	
	$("#kabupatenList2").change(function(){
		chkKab2();
		chkTotal();
	});
	
});