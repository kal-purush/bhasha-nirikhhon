jQuery(document).ready(function($){
	$('#addShipment').addClass('active');
	$('#addShipmentF').on('submit', function(e){
		e.preventDefault(); // prevent default form submit
		$('div#fadeOut').fadeIn();
		if($("#addShipmentF")[0].checkValidity()) {
			var today  			=	new Date();
			var formatedToday 	=   today.toYMD();
	      	$formSerialize 		= 	$('#addShipmentF').serialize();
	      	$consignmentNo		= 	$('input#consignmentNo').val();
	      	var lastlistVal 	=	$('div#listShipment tbody tr:last-child td:first-child input').val();
	      	var origin			=	$('input#origin').val();
	      	var destination		=	$('input#destination').val();
	      	var pickupDate 		=	$('input#pickupDate').val();
	      	var pickupTime 		= 	$('input#pickupTime').val();
	      	var status			=	$('select#status').val();
	      	var base_url		=	$('#base_url').val();

	      	var html 			=	'<tr><td><input type="checkbox" name="deleteChk" value="'+parseInt(lastlistVal + 1)+'"></td>'
	      							+ '<td>'+$consignmentNo+'</td>'
	      							+ '<td>'+origin+'</td>'
									+ '<td>'+destination+'</td>'
									+ '<td>'+formatedToday+' - ' + currentTime(today) + '</td>'
									+ '<td>'+status+'</td>'
									+ '<td><div class="edit"><a href="'+base_url+'admin.php?page=shipping&amp;edit='+$consignmentNo+'"><div alt="f464" class="dashicons dashicons-edit"></div></a></div></td>'
									+ '</tr>';

			 $.ajax({
	            type: "POST",
	            url: ajaxurl,
	            data: {
	                action: "chShipmentinsert",
	                formVar: $formSerialize,
	                consignmentNo: $consignmentNo
	            },
	            success: function(data) {
	                if(data.length <= 8){
	                	jQuery('.errorMessage').fadeOut();
	                	jQuery('div#fadeOut').fadeOut();
	                	jQuery('.successMessage').html(data);
	                	jQuery('.successMessage').fadeIn();
	                	jQuery('div#listShipment tbody').append(html);
	                	jQuery('#reset').click();
	                }else{
	        			jQuery('.errorMessage').html(data);        	
	        			jQuery('.errorMessage').fadeIn();
	        			$('div#fadeOut').fadeOut();
	                }
	            }
	        });
	    }else{
	    	$('.errorMessage').html("<p>Invalid info. fill up / empty below marked field's</p>");
	    	$('.errorMessage').fadeIn();
	    	$(':input[required]').addClass('error');
	    	$('div#fadeOut').fadeOut();
	    }
	});


	/*
	* Update Shipping Form
	*/
	$('#updateShipmentF').on('submit', function(e){
		e.preventDefault(); // prevent default form submit
		$('div#fadeOut').fadeIn();
		if($("#updateShipmentF")[0].checkValidity()) {
	      	$formSerialize 		= $('#updateShipmentF').serialize();
	      	var consignmentNo 	= $(this).find('input#consignmentNo').val();
	      	//Today
	      	var today = new Date();
			var dd = today.getDate();
			var mm = today.getMonth()+1; //January is 0!

			var yyyy = today.getFullYear();
			if(dd<10){
			    dd='0'+dd
			} 
			if(mm<10){
			    mm='0'+mm
			} 
			var today = dd+'/'+mm+'/'+yyyy;

	      	var location		=	$('input#newLocation').val();
	      	var newStatus 		=	$('select#newStatus').val();
	      	var deliveryTime	= 	$('input#deliveryTime').val();
	      	var comments		=	$('textarea#comments').val();
	      	var usHtml 			= 	'<tr>'
	      						+	'<td>'+today+'</td>'
	      						+	'<td>'+deliveryTime+'</td>'
	      						+	'<td>'+location+'</td>'
	      						+	'<td>'+newStatus+'</td>'
	      						+	'<td>'+comments+'</td>'
	      						+	'</tr>'
			 $.ajax({
	            type: "POST",
	            url: ajaxurl,
	            data: {
	                action: "chShipmentUpdate",
	                formVar: $formSerialize
	            },
	            success: function(data) {
	                if(data.length <= 8){
	                	jQuery('div#fadeOut').fadeOut();
	                	jQuery('.successMessage').html(data);
	                	jQuery('.successMessage').fadeIn();
	                	jQuery('.newStatusUpdate tbody').append(usHtml);
	                	jQuery('div#listShipment table tr#' + consignmentNo + ' td:nth-child(6)').text(newStatus);
	                	jQuery('div#reportShipment table tr#' + consignmentNo + ' td:nth-child(5)').text(newStatus);
	                	
	                	
	                }else{
	        			jQuery('.errorMessage').html(data);        	
	        			jQuery('.errorMessage').fadeIn();
	        			$('div#fadeOut').fadeOut();
	                }
	            }
	        });

	    }else{
	    	$('.errorMessage').html("<p>Invalid info. fill up / empty below marked field's</p>");
	    	$('.errorMessage').fadeIn();
	    	$(':input[required]').addClass('error');
	    	$('div#fadeOut').fadeOut();
	    }
	});


	/*
	* Delete Functionality 
	*/
	$(document.body).on('click', '#delete', function(event){
		event.preventDefault();
		var alldVal = $('div#listShipment tbody input:checkbox:checked').map(function(){
			return $(this).val();
		}).get();

		$.ajax({
			type: "POST", 
			url: ajaxurl,
			data: {
				action: 'chShipmentDelete',
				dArray: alldVal
			}, 
			success:function(data){
				for($i=0; $i<alldVal.length; $i+=1){
					jQuery('input[value="'+alldVal[$i]+'"]').closest('tr').remove();
				}
				$('a[href="#listShipment"]').click();
			}
		});
	});





	/*
	* Delete Permanently Functionality 
	*/
	$(document.body).on('click', '#deleteDelivered', function(event){
		event.preventDefault();
		var alldValf = $('#delivered tbody input:checkbox:checked').map(function(){
			return $(this).val();
		}).get();
		console.log(alldValf);
		$.ajax({
			type: "POST", 
			url: ajaxurl,
			data: {
				action: 'chShipmentDelete',
				dArray: alldValf
			}, 
			success:function(data){
				for($i=0; $i<alldValf.length; $i+=1){
					jQuery('input[value="'+alldValf[$i]+'"]').closest('tr').remove();
				}
				$('a[href="#delivered"]').click();
			}
		});
	});




	/*
	* Search Shipping data
	*/
	$('form#bookingSForm, #bookingSForm2').on('submit', function(e){
		var thisF = $(this);
		e.preventDefault(); // prevent default form submit
		$searchVal = $('input#search').val();
		var sHtml = '';
		if(!$searchVal){
			jQuery('input#search').addClass('error');
			return false;
		}
			 $.ajax({
	            type: "POST",
	            url: ajaxurl,
	            dataType: "json",
	            data: {
	                action: "chShipmentSearch",
	                sVal: $searchVal
	            },
	            success: function(data) {
	              
	                	jQuery('input#search').removeClass('error');
	                	jQuery('.errorMessage').html(data);
	                	console.log(data);
	                	jQuery('.errorMessage').show();
	                	//console.log('succes');

	                	for($s = 0; $s < data.length; $s+=1 ){
	                		console.log(data[$s].base_url);
	                		sHtml 	+= '<tr id="'+data[$s].consignmentNo+'">'
									+ '<td><input type="checkbox" name="deleteChk[]" value="'+data[$s].s_id+'"></td>'
									+ '<td>'+data[$s].consignmentNo+'</td>'
									+ '<td>'+data[$s].origin+'</td>'
									+ '<td>'+data[$s].destination+'</td>'
									+ '<td>'+ new Date(data[$s].insert_date).toYMD()+ ' '+ currentTime( new Date(data[$s].insert_date) ) +'</td>'
									+ '<td>'+data[$s].status+'</td>'
									+ '<td><div class="edit"><a href="'+data[$s].base_url+'admin.php?page=shipping&amp;edit='+data[$s].consignmentNo+'"><div alt="f464" class="dashicons dashicons-edit"></div></a></div></td>'
									+ '</tr>';
	                	}
	                	jQuery(thisF).closest('.booking_search').next('table').find('tbody').fadeOut('fast');
	                	jQuery(thisF).closest('.booking_search').next('table').find('tbody').html(sHtml);
	                	jQuery(thisF).closest('.booking_search').next('table').find('tbody').slideDown();
	            }, 
	            error: function(xhr, status, error) {
      		console.log('error search');
   			}
	        });
	});



	/*
	* Quick Form Insert
	*/
	$('#quickForm').on('submit', function(e){
		e.preventDefault(); // prevent default form submit
		$('div#fadeOut').fadeIn();
		if($("#quickForm")[0].checkValidity()) {
	      	$qFormData 		= $('#quickForm').serialize();
	      	//Today
	      	var today = new Date();
			var fToday = today.toYMD();

			var base_url 			= 	$('#base_url').val();
	      	var tracking_number		=	$('input#tracking_number').val();
	      	var q_origin 			=	$('#quickForm input#origin').val();
	      	var q_destination		= 	$('#quickForm input#destination').val();
	      	var q_weight			=	$('#quickForm input#weight').val();
	      	var q_charge			=	$('#quickForm input#charge').val();
	      	var q_status			=	$('#quickForm select#status').val();

	      	var qHtml 				=	'<tr id="'+ tracking_number.split(' ').join('') +'">'
	      							+	'<td>'+tracking_number+'</td>'
	      							+	'<td>'+q_origin+'</td>'
	      							+	'<td>'+q_destination+'</td>'
	      							+	'<td>'+fToday+'</td>'
	      							+	'<td>'+q_status+'</td>'
	      							+	'<td><a href=" '+base_url+'admin.php?page=shipping&amp;compete='+ tracking_number +' ">Make Complete</a> | <a href="javascript:void(0)" class="deleteQuick" data-action="'+ tracking_number +'">Delete</a></td>'
	      							+	'</tr>'
			 $.ajax({
	            type: "POST",
	            url: ajaxurl,
	            data: {
	                action: "quickShipment",
	                quickForm: $qFormData,
	                truckNo: tracking_number
	            },
	            success: function(data) {
	                if(data.length <= 8){
	                	jQuery('div#fadeOut').fadeOut();
	                	jQuery('.successMessage').html(data);
	                	jQuery('.successMessage').fadeIn();
	                	jQuery('#quickLists tbody').append(qHtml);
	                	
	                }else{
	        			jQuery('.errorMessage').html(data);        	
	        			jQuery('.errorMessage').fadeIn();
	        			$('div#fadeOut').fadeOut();
	                }
	            }
	        });

	    }else{
	    	$('.errorMessage').html("<p>Invalid info. fill up / empty below marked field's</p>");
	    	$('.errorMessage').fadeIn();
	    	$(':input[required]').addClass('error');
	    	$('div#fadeOut').fadeOut();
	    }
	});



	/*
	Delete Quick 
	*/
	$(document.body).on('click', 'a.deleteQuick', function(){
		jQuery('div#fadeOut').fadeIn();
		$deleteQuick = $(this).data('action');
		$deleteQuickId = $deleteQuick.split(' ').join('');
			$.ajax({
	            type: "POST",
	            url: ajaxurl,
	            data: {
	                action: "deleteQuick",
	                quickId: $deleteQuick,
	            },
	            success: function(data) {
	                if(data.length <= 8){
	                	jQuery('div#fadeOut').fadeOut();
	                	$('#'+$deleteQuickId).remove();
	                }else{
	        			$('div#fadeOut').fadeOut();
	                }
	            }
	        });
	});


	/*
	* Menu Functionality
	*/
	$(document.body).on('click', '.innerMain .head ul li a', function(){
		var tattr = $(this).attr('href');
		console.log(tattr);
		$('.innerMain .head ul li a').removeClass('active');
		$(this).addClass('active');
		$('.bodyInner').hide();
		$(tattr).fadeIn();
		return false;
	});			$(document.body).on('click', '.nav-tabs li a', function(){				var attrh = $(this).attr('href');		$('.step-content').hide();		$('.nav-item').removeClass('active');		$(this).closest('li').addClass('active');		$(attrh).fadeIn();		return false;	});

	/*
	* Check All
	*/
	$(document.body).on('change', 'input[name="checkAll"]', function(){
		if($(this).is(':checked')){
			$(this).closest('thead').next('tbody').find('input[name="deleteChk[]"]').prop( "checked", true );
		}else{
			$(this).closest('thead').next('tbody').find('input[name="deleteChk[]"]').prop( "checked", false );
		}
	});
  
  if(window.location.hash == '#listShipment') {
  		$('a[href="#listShipment"]').click();
	}

	
	$(document.body).on('click', 'a.viewReport', function(){
		/*
			* View Report Function
		*/
		$('.reportView').remove();
		var tr = $(this).closest('tr');
		var currentS = $(tr).data('currentstatus');
		console.log(currentS);
		var cStatus = '';				
		for(var z=0; z < currentS.length; z++){
			var fdate = new Date(currentS[z].deliveryDate);
			var str = fdate.toYMD();
					cStatus +=	'<tr>'
							+	'<td>' + str + '</td>'
							+	'<td>'+currentS[z].deliveryTime+'</td>'
							+	'<td>'+currentS[z].newLocation+'</td>'
							+	'<td>'+currentS[z].newStatus+'</td>'
							+	'<td>'+currentS[z].comments+'</td>'
							+	'</tr>';
						}
		var newHtml = 		'<div class="reportView" style="display:none;"><div class="updateStatis pt50">'
						+	'<div class="hideReport"><div alt="f153" class="dashicons dashicons-dismiss"></div></div>'
						+ 	'<h3 class="mt30">Tracking Shipment</h3>'
						+	'<div class="topTable">'
						+	'<table align="center" cellpadding="2" cellspacing="2" bgcolor="#EEEEEE">'			
						+	'<tbody><tr>'
						+	'<td align="right" bgcolor="#FFFFFF" class="Partext1"><div align="center">'
						+	'<table width="80%" border="0" cellspacing="1" cellpadding="1">'
						+	'<tbody><tr>'
						+	'<td width="55%"><div align="left">Shipper Name : </div></td>'
						+	'<td width="45%"><div align="left">'+$(tr).data('shippername')+'</div></td>'
						+	'</tr>'
						+	'<tr>'
						+	'<td><div align="left">Shipper Phone : </div></td>'
						+	'<td><div align="left">'+$(tr).data('shippertel')+'</div></td>'
						+	'</tr>'
						+	'<tr>'
						+	'<td><div align="left">Shipper Address : </div></td><td>'+$(tr).data('shipperaddress')+'</div></td>'
						+	'</tr>'
						+	'</tbody></table>'
						+	'</div></td>'
						+	'<td bgcolor="#FFFFFF" class="Partext1"><div align="center">'
						+	'<table width="80%" border="0" cellspacing="1" cellpadding="1">'
						+	'<tbody><tr>'
						+	'<td width="55%"><div align="left">Receiver Name : </div></td>'
						+	'<td width="45%"><div align="left">'+$(tr).data('receivername')+'</div></td>'
						+	'</tr>'
						+	'<tr>'
						+	'<td><div align="left">Receiver Phone : </div></td>'
						+	'<td><div align="left">'+$(tr).data('receivertel')+'</div></td>'
						+	'</tr>'
						+	'<tr>'
						+	'<td><div align="left">Receiver Address : </div></td>'
						+	'<td><div align="left">'+$(tr).data('receiveraddress')+'</div></td>'
						+	'</tr>'
						+	'</tbody></table>'
						+	'</div></td>'
						+	'</tr>'
						+	'<tr>'
						+	'<td align="right" bgcolor="#FFFFFF" class="Partext1">&nbsp;</td>'
						+	'<td bgcolor="#FFFFFF" class="Partext1">&nbsp;</td>'
						+	'</tr>'
						+	'<tr>'
						+	'<td width="336" align="right" bgcolor="#FFFFFF" class="Partext1">Consignment No  : </td>'
						+	'<td width="394" bgcolor="#FFFFFF" class="Partext1">'+$(tr).data('consignmentno')+'</td>'
						+	'</tr>'
						+	'<tr>'
						+	'<td align="right" bgcolor="#FFFFFF" class="Partext1">Ship Type  :</td>'
						+	'<td bgcolor="#FFFFFF" class="Partext1">'+$(tr).data('typeofshipment')+'</td>'
						+	'</tr>'
						+	'<tr>'
						+	'<td align="right" bgcolor="#FFFFFF" class="Partext1">Weight :</td>'
						+	'<td bgcolor="#FFFFFF" class="Partext1">'+$(tr).data('weight')+' (kg)</td>'
						+	'</tr>'	
						+	'<tr>'
						+	'<td align="right" bgcolor="#F3F3F3" class="Partext1">Invoice no  :</td>'
						+	'<td bgcolor="#FFFFFF" class="Partext1">'+$(tr).data('invoiceno')+'</td>'
						+	'</tr>'
						+	'<tr>'
						+	'<td align="right" bgcolor="#F3F3F3" class="Partext1">Total freight : </td>'
						+	'<td bgcolor="#FFFFFF" class="Partext1">'+$(tr).data('totalfreight')+'</td>'
						+	'</tr>'
						+	'<tr>'
						+	'<td align="right" bgcolor="#F3F3F3" class="Partext1">Mode : </td>'
						+	'<td bgcolor="#FFFFFF" class="Partext1">'+$(tr).data('mode')+'</td>'
						+	'</tr>'
						+	'<tr>'
						+	'<td align="right" bgcolor="#FFFFFF" class="Partext1">Origin : </td>'
						+	'<td bgcolor="#FFFFFF" class="Partext1">'+$(tr).data('origin')+'</td>'
						+	'</tr>'
						+	'<tr>'
						+	'<td align="right" bgcolor="#FFFFFF" class="Partext1">Destination :</td>'
						+	'<td bgcolor="#FFFFFF" class="Partext1">'+$(tr).data('destination')+'</td>'
						+	'</tr>'
						+	'<tr>'
						+	'<td align="right" bgcolor="#FFFFFF" class="Partext1">Pickup Date/Time  :</td>'
						+	'<td bgcolor="#FFFFFF" class="">'+$(tr).data('pickupdate')+'</td>'
						+	'</tr>'
						+	'<tr>'
						+	'<td align="right" bgcolor="#FFFFFF" class="Partext1">Status :</td>'
						+	'<td bgcolor="#FFFFFF" class="Partext1">'+$(tr).data('status')+'</td>'
						+	'</tr>'
						+	'<tr>'
						+	'<td align="right" valign="top" bgcolor="#FFFFFF" class="Partext1">Comments :</td>'
						+	'<td bgcolor="#FFFFFF" class="Partext1">'+$(tr).data('comments')+'</td>'
						+	'</tr>'
						+	'</tbody></table>'
						+	'</div>'
						+	'<div class="newStatusUpdate pt50 pt20">'
						+	'<table class="table">'
						+	'<thead>'
						+	'<tr>'
						+	'<th>Date</th>'
						+	'<th>Time</th>'	
						+	'<th>Location</th>'
						+	'<th>Status</th>'
						+	'<th>Comments</th>'
						+	'</tr>'
						+	'</thead>'
						+	'<tbody> '+cStatus+'';
						+	'</tbody>'
						+	'</table>'
						+	'</div>'
						+	'</div></div>';
		$('.mainWrap .innerMain').append(newHtml);
		$('.reportView').toggle( "fold" );
	});//report view
	


	$(document.body).on('click', '.reportView .hideReport > div', function(){
		
		$('.reportView').toggle( "fold" )
	});

}); // End Document Ready


 jQuery( function() {
      jQuery( "#pickupDate, #expectedDlyDate, input#deliveryDate" ).datepicker({ dateFormat: 'dd/mm/yy' });
 } );


/* Time Picker */
 jQuery(function() {
   jQuery('input#pickupTime, input#deptTime, input#deliveryTime, input#deliveryTime').timepicker();
 });


// Date Function 
(function() {
    Date.prototype.toYMD = Date_toYMD;
    function Date_toYMD() {
        var year, month, day;
        year = String(this.getFullYear());
        month = String(this.getMonth() + 1);
        if (month.length == 1) {
            month = "0" + month;
        }
        day = String(this.getDate());
        if (day.length == 1) {
            day = "0" + day;
        }
        return day + "/" + month + "/" + year;
    }
})();

 //Current Time
 function currentTime(date) {
  var hours = date.getHours();
  var minutes = date.getMinutes();
  var ampm = hours >= 12 ? 'PM' : 'AM';
  hours = hours % 12;
  hours = hours ? hours : 12; // the hour '0' should be '12'
  minutes = minutes < 10 ? '0'+minutes : minutes;
  var strTime = hours + ':' + minutes + ' ' + ampm;
  return strTime;
}

function nextPrevious(step){
if(step=='mailSummary'){	
			if(jQuery('#addressee').val()==""){
				alert('Please enter addressee');
				return false;
			}
			if(jQuery('#address1').val()==""){
				alert('Please enter address1');
				return false;
			}
			if(jQuery('#amount').val()==""){
				alert('Please enter amount');
				return false;
			}
			if(jQuery('#city').val()==""){
				alert('Please enter city');
				return false;
			}
			if(jQuery('#weight_kg').val()==""){
				alert('Please enter Weight(kilogram)');
				return false;
			}

}

	    jQuery('.step-content').hide();		jQuery('.nav-item').removeClass('active');		jQuery('.'+step).addClass('active');		jQuery('#'+step).fadeIn();	
			if(step=='mailSummary'){	
	
			jQuery('#mail_creation_form input[type="text"], #mail_creation_form select').each(				function(index){  					var input = jQuery(this);										console.log( '#td_'+input.attr('id') );					jQuery('#td_'+input.attr('id') ).html( jQuery(this).val() );				}			);				}
			
			}function weightConverter(valNum) { 
 jQuery('#weight_grm').val(valNum*1000);}
 function savemail(){	
 var dataAll = jQuery( "#mail_creation_form" ).serialize();
 jQuery.ajax({	
 type: "POST",	
 url: ajaxurl,	
 data:  dataAll,
 success: function(data) {	
	alert('Your data has been saved');
 }	
 });}

function nextPreviousbatch(step){
	
	if(step=='batchupload_form_print'){
	
		if(!jQuery('input[name="confirm_batch"]').is(":checked") ){
			alert('Please agree to terms by clicking checkbox ');
			return false;
		}
		
	}
	
    jQuery('.batchuploadsection .step-content').hide();	
	jQuery('.batchuploadsection .nav-item').removeClass('active');	
	jQuery('.'+step).addClass('active');
	jQuery('#'+step).fadeIn();	
}


	
	
jQuery( document ).ready(function() {	
	jQuery('#uploadbatch_form').validate({
		rules: {
					batchupload: "required",
								
			},
		submitHandler: function(form) {

			
			var formdata = new FormData(jQuery("#uploadbatch_form")[0]);
			jQuery.ajax({	
				type: "POST",
				url: ajaxurl,
				data:  formdata,
				contentType: false,
				processData: false	,
				success: function(data) {
				
					//alert('as');
					jQuery("#batch_upload_edit tbody tr").remove(); 
					jQuery("#batch_upload_edit tbody").append(data);
					nextPreviousbatch('batchupload_form_edit');
				}	
			});
		}	
			
		
	});
});


function batchsummary(){
		
		console.log(jQuery('input[name="batch_name"]').length);
			if(jQuery('input[name="batch_name"]').length==0 ){
				alert('Please upload batch file');
				return false;
			}
		
			var formdata = jQuery("#uploadbatch_formedit").serialize();;
			jQuery.ajax({	
				type: "POST",
				url: ajaxurl,
				data:  formdata,
				success: function(data) {
				
					//alert('as');
					jQuery("#batch_upload_review tbody").remove(); 
					jQuery("#batch_upload_review").append(data);
					nextPreviousbatch('batchupload_form_confirm');
				}	
			});
			
	
}

function printmail(){
	
	jQuery( "#mail_action").val("printmail");
	var formdata = jQuery("#mail_creation_form").serialize();
	jQuery( "#mail_action").val("mail_create_save");
	console.log(formdata);
			jQuery.ajax({	
				type: "POST",
				url: ajaxurl,
				data:  formdata,
				success: function(data) {
				
					//alert('as');
					//jQuery("#batch_upload_review tbody").remove(); 
					//jQuery("#batch_upload_review").append(data);
					//nextPreviousbatch('batchupload_form_confirm');
					//window.open();
					 window.open("","_blank",'location=no,height=570,width=700,scrollbars=yes,status=yes').document.write(data);
					//w.document.write(data);
				}	
			});
	
}

function printbatchmail(){

	jQuery( "#batch_uploadreview_action").val("printbatchmail");
	var formdata = jQuery("#uploadbatch_formedit").serialize();
	jQuery( "#batch_uploadreview_action").val("batch_uploadreview");
	console.log(formdata);
			jQuery.ajax({	
				type: "POST",
				url: ajaxurl,
				data:  formdata,
				success: function(data) {
				
					
					 window.open("","_blank",'location=no,height=570,width=700,scrollbars=yes,status=yes').document.write(data);
					
				}	
			});
}

function savebatch(){

	jQuery( "#batch_uploadreview_action").val("savebatchmail");
	var formdata = jQuery("#uploadbatch_formedit").serialize();
	jQuery( "#batch_uploadreview_action").val("batch_uploadreview");
	
	jQuery.ajax({	
				type: "POST",
				url: ajaxurl,
				data:  formdata,
				success: function(data) {				
					
					 //alert('Your data has been saved');
					  jQuery("#uploadbatch_formedit")[0].reset();
					  if(data=='done'){
						alert('Please process another file.')
					  }
					  if(data=='yes'){
						alert('Your data has been saved');
					  }
					  if(data=='all'){
						alert('This batch is already uploaded');
					  }
					
				}	
			});
}

function tagprocess(){
	
	
	var formdata = jQuery(".mail_process_form").serialize();
	
	jQuery.ajax({	
				type: "POST",
				url: ajaxurl,
				data:  formdata,
				dataType: 'json',
				success: function(data) {				
					
					jQuery(".nav-tabs .nav-item").removeClass('active');
					jQuery(".nav-tabs .nav-item:nth-child(2)").addClass('active');
					jQuery("#step-1").hide();
					jQuery("#step-2").show();
					jQuery("#mail_tagging_output_tbody").empty();
					jQuery("#mail_tagging_output_tbody").append(data[0]);
					jQuery("#tag_section_summary").empty();
					jQuery("#tag_section_summary").append(data[1]);
					jQuery("#batchnumber").empty();
					jQuery("#batchnumber").append(data[2]);
					
				}	
			});
	
}



function tag_step(stepid){
//alert(stepid);
	jQuery(".nav-tabs .nav-item").removeClass('active');
	jQuery(".nav-tabs .nav-item:nth-child("+stepid+")").addClass('active');
	jQuery(".step-content").hide();
	jQuery("#step-"+stepid).show();
}


function tag_summary(){

		jQuery( '#tag_process_form_action_field').val("tag_process_summary");
		var formdata = jQuery(".tag_process_form").serialize();
		//jQuery( '#tag_process_form_action_field').val("save_tag");
		jQuery.ajax({	
				type: "POST",
				url: ajaxurl,
				data:  formdata,
				success: function(data) {				
					//alert(data);
					//mail_tagging_output_summary
					jQuery("#mail_tagging_output_summary").empty();
					jQuery("#mail_tagging_output_summary").append(data);
					//alert('Data is saved succesfully');
					jQuery(".nav-tabs .nav-item").removeClass('active');
					jQuery(".nav-tabs .nav-item:nth-child(3)").addClass('active');
					jQuery(".step-content").hide();
					jQuery("#step-3").show();
					
				}	
			});
		
}

function savetagging(){

jQuery( '#tag_process_form_action_field').val("save_tag");
var formdata = jQuery(".tag_process_form").serialize();
	
	jQuery.ajax({	
				type: "POST",
				url: ajaxurl,
				data:  formdata,
				success: function(data) {				
					
					alert('Data is saved succesfully');
					location.reload();
					
				}	
			});
	
}

function tag_section_summary_button(){
	var tag_section_tracking_input_id = jQuery("#tag_section_tracking_input_id").val();
	jQuery.ajax({	
				type: "POST",
				url: ajaxurl,
				data:  'tag_section_tracking_input_id='+tag_section_tracking_input_id+'&action=showsummaryintag',
				
				success: function(data) {					
					
					jQuery("#tag_section_summary").empty();
					jQuery("#tag_section_summary").append(data);
					jQuery("#mail_tagging_output #"+tag_section_tracking_input_id+" input[type='text']").val('good');
					jQuery("#tag_section_tracking_input_id").val('');
					
				}	
			});
}

function keyChecking() {
				//alert("key");
	

	var myLength = jQuery("#tag_section_tracking_input_id").val().length;
	//console.log(myLength);
	
	if(myLength >= 13){
		
		tag_section_summary_button();
		
		
		
	}

}






 
