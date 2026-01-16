jQuery(function($) {
	
	//получаем кол-во миниатюр у блока
	function getImageNum(block) {
		var count_items = 0,
			pItems = $('#' + block + ' li'),
			iLength = pItems.length,
			i = 0;
		
		for(i = 0; i < iLength; i++)	{
			if($(pItems[i]).data('item') != $(pItems[i]).html()) {
				count_items++;
			} else {
				count_items++;
				break;
			}
		}
		return count_items;
	}
	
	//переупорядочиваем после удаления список миниатюр у блока
	function reorderImages(block) {
		var pItems = $('#' + block + ' li'),
			iLength = pItems.length,
			i = 0;
		
		if(iLength > 0)	{
			for(i = 0; i < iLength; i++)	{
				if($(pItems[i]).data('item') == $(pItems[i]).html() && $(pItems[i+1]).data('item') != $(pItems[i+1]).html()) {
					$(pItems[i]).html($(pItems[i+1]).html());
					$(pItems[i+1]).html($(pItems[i+1]).data('item'));
					//$(pItems[i]).toggleClass('no-foto');
					//$(pItems[i+1]).toggleClass('no-foto');
				}
			}
		}
	}
	
	
	var upload_other_file_item_num = getImageNum('uploading-other-file-list'),
		//upload_example_item_num = getImageNum('uploading-examples-list'),
		clicked_el = null;
		
	if($("div").is("#uploading-passport-file")) {
		var upload_passport_file = new AjaxUpload('#upload-passport-file-btn', {
			action: '/ajax/upload-passport-file',
			name: 'UploadPassportForm[imageFiles]',
			onSubmit : function(file, extension){
				$("#loading-passport-file").show();
				$('#loading-passport-file-errormes').html('');
				upload_passport_file.setData({'file': file});
				$("#loading-passport-file-success").hide();
				$("#loading-passport-file-success").find('input[type="hidden"]').remove();
			},
			onComplete : function(file, response){
				$("#loading-passport-file").hide();

				response = $.parseJSON(response);

				if(response.res == 'ok') {
					$("#loading-passport-file-success").show();
					$("#loading-passport-file-success").append(response.filename);
				} else {
					for (var i = 0; i < response.msg.length; i++) {
					   $('#loading-passport-file-errormes').append(response.msg[i]);
					}
				}
			}
		});
	}
	
	if($("div").is("#uploading-trud-book-file")) {
		var upload_trud_book_file = new AjaxUpload('#upload-book-file-btn', {
			action: '/ajax/upload-book-file',
			name: 'UploadBookForm[imageFiles]',
			onSubmit : function(file, extension){
				$("#loading-book-file").show();
				$('#loading-book-file-errormes').html('');
				upload_trud_book_file.setData({'file': file});
				$("#loading-book-file-success").hide();
				$("#loading-book-file-success").find('input[type="hidden"]').remove();
			},
			onComplete : function(file, response){
				$("#loading-book-file").hide();

				response = $.parseJSON(response);

				if(response.res == 'ok') {
					$("#loading-book-file-success").show();
					$("#loading-book-file-success").append(response.filename);
				} else {
					for (var i = 0; i < response.msg.length; i++) {
					   $('#loading-book-file-errormes').append(response.msg[i]);
					}
				}
			}
		});
	}
	
	if($("div").is("#uploading-diplom-file")) {
		var upload_diplom_file = new AjaxUpload('#upload-diplom-file-btn', {
			action: '/ajax/upload-diplom-file',
			name: 'UploadDiplomForm[imageFiles]',
			onSubmit : function(file, extension){
				$("#loading-diplom-file").show();
				$('#loading-diplom-file-errormes').html('');
				upload_diplom_file.setData({'file': file});
				$("#loading-diplom-file-success").hide();
				$("#loading-diplom-file-success").find('input[type="hidden"]').remove();
			},
			onComplete : function(file, response){
				$("#loading-diplom-file").hide();

				response = $.parseJSON(response);

				if(response.res == 'ok') {
					$("#loading-diplom-file-success").show();
					$("#loading-diplom-file-success").append(response.filename);
				} else {
					for (var i = 0; i < response.msg.length; i++) {
					   $('#loading-diplom-file-errormes').append(response.msg[i]);
					}
				}
			}
		});
	}

	if($("div").is("#uploading-other-file")) {
		var upload_other_file = new AjaxUpload('#upload-other-file-btn', {
			action: '/ajax/upload-documents-other',
			name: 'UploadDocumentsOtherForm[imageFiles]',
			onSubmit : function(file, extension){
				$("#loading-other-file").show();
				$('#loading-other-file-errormes').html('');
				$("#loading-other-file-success").hide();
				upload_other_file.setData({'file': file});
			},
			onComplete : function(file, response){
				$("#loading-other-file").hide();
				$("#loading-other-file-success").show();

				response = $.parseJSON(response);

				if(response.res == 'ok') {
					$('#uploading-other-file-list ul').append(response.filename);
					
					upload_other_file_item_num++;
					if(upload_other_file_item_num > 9)	$('#upload-other-file-btn').css('visibility', 'hidden');
				} else {
					for (var i = 0; i < response.msg.length; i++) {
					   $('#loading-other-file-errormes').append(response.msg[i]);
					}
				}
			}
		});
	}
	

	if($("div").is("#uploading-reg-file")) {
		var upload_reg_file = new AjaxUpload('#upload-reg-file-btn', {
			action: '/ajax/upload-reg-file',
			name: 'UploadRegFileForm[imageFiles]',
			onSubmit : function(file, extension){
				$("#loading-reg-file").show();
				$('#loading-reg-file-errormes').html('');
				upload_reg_file.setData({'file': file});
			},
			onComplete : function(file, response){
				$("#loading-reg-file").hide();

				response = $.parseJSON(response);

				if(response.res == 'ok') {
					$("#loading-reg-file-success").show();
					//$("#loading-reg-file-success").append(response.filename);
					$("#uploading-reg-file").find('input[name="'+response.filename+'"]').val(response.filevalue);
				} else {
					for (var i = 0; i < response.msg.length; i++) {
					   $('#loading-reg-file-errormes').append(response.msg[i]);
					}
				}
			}
		});
	}
	
	if($("div").is("#uploading-license")) {
		var upload_license = new AjaxUpload('#upload-license-btn', {
			action: '/ajax/upload-license',
			name: 'UploadLicenseForm[imageFiles]',
			onSubmit : function(file, extension){
				$("#loading-license").show();
				$('#loading-license-errormes').html('');
				upload_license.setData({'file': file});
			},
			onComplete : function(file, response){
				$("#loading-license").hide();

				response = $.parseJSON(response);

				if(response.res == 'ok') {
					$("#loading-license-success").show();
					$("#loading-license-success").append(response.filename);
				} else {
					for (var i = 0; i < response.msg.length; i++) {
					   $('#loading-license-errormes').append(response.msg[i]);
					}
				}
			}
		});
	}
	
	if($("div").is("#uploading-bitovie-file")) {
		var upload_bitovie_file = new AjaxUpload('#upload-bitovie-file-btn', {
			action: '/ajax/upload-bitovie-file',
			name: 'UploadBitovieFileForm[imageFiles]',
			onSubmit : function(file, extension){
				$("#loading-bitovie-file").show();
				$('#loading-bitovie-file-errormes').html('');
				upload_bitovie_file.setData({'file': file});
			},
			onComplete : function(file, response){
				$("#loading-bitovie-file").hide();

				response = $.parseJSON(response);

				if(response.res == 'ok') {
					$("#loading-bitovie-file-success").show();
					$("#loading-bitovie-file-success").append(response.filename);
				} else {
					for (var i = 0; i < response.msg.length; i++) {
					   $('#loading-bitovie-file-errormes').append(response.msg[i]);
					}
				}
			}
		});
	}
	
	if($("div").is("#uploading-attestat-file")) {
		var upload_attestat_file = new AjaxUpload('#upload-attestat-file-btn', {
			action: '/ajax/upload-attestat-file',
			name: 'UploadAttestatForm[imageFiles]',
			onSubmit : function(file, extension){
				$("#loading-attestat-file").show();
				$('#loading-attestat-file-errormes').html('');
				upload_attestat_file.setData({'file': file});
			},
			onComplete : function(file, response){
				$("#loading-attestat-file").hide();

				response = $.parseJSON(response);

				if(response.res == 'ok') {
					$("#loading-attestat-file-success").show();
					$("#loading-attestat-file-success").append(response.filename);
				} else {
					for (var i = 0; i < response.msg.length; i++) {
					   $('#loading-attestat-file-errormes').append(response.msg[i]);
					}
				}
			}
		});
	}
	
	
	$('#uploading-other-file').on('click', '.remove-uploaded-file', function(e){
		e.preventDefault();
		$(this).parent().html($(this).parent().data('item'));
				
		reorderImages('uploading-other-file-list');
		upload_other_file_item_num = getImageNum('uploading-other-file-list');
		
		var pItems = $('#uploading-other-file-list li');		
		$(pItems[upload_other_file_item_num - 1]).toggleClass('no-foto');
		
		if(upload_other_file_item_num < 10)	$('#upload-other-file-btn').css('visibility', 'visible');
		return false;
	});
	
	$('.remove-document-file').on('click', function(e){
		e.preventDefault();
		$(this).parent().find('.document_delete__popup').show();
		return false;
	});
	
	$('.document-delete-no').on('click', function(e){
		e.preventDefault();
		$(this).parent().parent().hide();
		return false;
	});
	
	$('.document-delete-yes').on('click', function(e){
		var data_file = $(this).data('file');
		e.preventDefault();
		
		if($(this).parent().parent().parent().parent().parent().hasClass('uploading-other-file-list')) {
			$(this).parent().parent().parent().remove();
		} else {
			$(this).parent().parent().parent().parent().parent().find('input[type="hidden"]').each(function(){			
				if($(this).attr('name').indexOf(data_file) != -1) $(this).val('');
			});
			
			$(this).parent().parent().hide();
			$(this).parent().parent().parent().hide();
			
		}
		
		return false;
	});
	
	$(document).click(function(event) {
		if ($(event.target).closest(".document_delete__popup").length) return;
		
		if($(".document_delete__popup").is(':visible'))
		   $(".document_delete__popup").hide();
		
		event.stopPropagation();
	});	
	
	
	
});