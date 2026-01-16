	$("#buy_quantity").keyup(function(e){
				var regNumber = /^([0-9]{2})$/;
				var str = $(this).val();
				if(!regNumber.test(str)) {
					var res = str.substring(0, str.length-1);
					$(this).val(res);
				}
			});
			
	// 상품 구매 관련
	function buyProduct() {
		var member_id = $("#member_id").val();
		var prd_id = $("#prd_id").val();
		var buy_quantity = $("#buy_quantity").val();
		var prd_opt = $("#prd_opt").val();
		if (member_id == "Administrator") {
			swal("", "관리자 권한으로는 이용하실 수 없습니다.", "warning");
		} else if (!member_id) {
			swal("", "비회원은 이용하실 수 없습니다.", "error");
		} else if (buy_quantity == 0) {
			swal("", "수량을 선택해주세요", "error");
		} else {
			$.ajax({
				type : "POST",
				url : "hotel_payment",
				data : {
					"member_id" : member_id,
					"prd_id" : prd_id,
					"buy_quantity" : buy_quantity,
					"prd_opt" : prd_opt
				},
				success : function(data) {
					console.log(data);
					window.location.href="hotel_payment";
				}, error : function(data) {
					console.log(data);
				}
			});
		}
	}
	
	
			//전체 선택 및 해제
	function check_all() {
			 // 전체 체크박스
			  const checkAll = document.getElementById("checkAll");

			  // 각 체크박스
			  const checkboxes = document.getElementsByName("chk");

			  // 전체 체크박스 체크되면
			  if (checkAll.checked) {
			    
				 // 각 체크박스 선택
			    for (let i = 0; i < checkboxes.length; i++) {
			      checkboxes[i].checked = true;
			    }
				 
			  } else {
			    // 모든 체크박스 선택 해제
			    for (let i = 0; i < checkboxes.length; i++) {
			      checkboxes[i].checked = false;
			    }
			  }
			}
			
			// 개별 체크박스 해제 시 전체박스 해제
			$(document).ready(function() {
			  var checkAll = document.getElementById("checkAll");
			  var checkboxes = document.getElementsByName("chk");

			  function onoffCheckItem() {
			    var allChecked = true;
			    for (var i = 0; i < checkboxes.length; i++) {
			      if (!checkboxes[i].checked) {
			        allChecked = false;
			        break;
			      }
			    }
			    checkAll.checked = allChecked;
			  }

			  for (var i = 0; i < checkboxes.length; i++) {
				  checkboxes[i].addEventListener('click', onoffCheckItem);
			  };
			});
		
			
			
			// 카드번호 숫자입력
			function onlyNumber(){
				if((event.keyCode<48)||(event.keyCode>57)) {
					event.returnValue=false;
				}
			}
			
			function termChk() {
				var checkAll = $("#checkAll").is(":checked");
				var member_id = $("#member_id").val();
				var prd_id = $("#prd_id").val();
				var buy_quantity = $("#buy_quantity").val();
				var member_phone = $("#member_phone").val();
				var credit_number = $("#credit1").val()+"-"+$("#credit2").val()+"-"+$("#credit3").val()+"-"+$("#credit4").val();
				jQuery.ajaxSettings.traditional = true;
				
					
				var chkValue = $('input[type = radio][name = payway]:checked').val();
				
				if(member_id == 'admin') {
					swal("관리자는 돌아가세요", "관리자는 상품을 직접 구매할 수 없습니다.", "error")
					return false;
				} else if(member_id == "" || !member_id) {
					swal("회원 전용상품", "구매를 위해 로그인 해주시기 바랍니다.", "error")
					return false;
					
				} else if(chkValue == 'cash') {
					swal("결제 오류", "이 상품은 무통장입금을 지원하지 않습니다.", "error")
					return false;
					
				} else if ($("#credit1").val().length != 4 || $("#credit2").val().length != 4 || $("#credit3").val().length != 4 || $("#credit4").val().length != 4) {
					swal("입력 오류", "카드번호를 확인하세요", "error")
					return false;		
					
				} else if (checkAll != true) {
					swal("", "약관에 동의해야 합니다.", "error")
					return false;			
					
				} else if (checkAll == true && $("#credit1").val() != "" && $("#credit2").val() != "" && $("#credit3").val() != "" && $("#credit4").val() != "") {
 					swal({
						icon : "info", 
						text : "결제를 진행 하시겠습니까?", 
						closeOnClickOutside : false, 
						closeOnEsc : false, 
						buttons : [ "취소", "결제" ],
					}).then(function(isConfirm) {
						if (isConfirm) {
							swal("결제 성공", "결제를 완료했습니다.", "success").then(function(isConfirm) {
								$.ajax({
									type : "POST", 
									url : "HotelPayment", 
									data : {
										"prd_id" : prd_id,
										"member_id" : member_id,
										"member_phone" : member_phone,
										"credit_number" : credit_number,
										"buy_quantity" : buy_quantity
									}, success : function(data) { window.location.href = "main"; }
								});
								console.log(prd_id);
							});
						}
					});
				}
			}
			
			
	$(document).ready(function() {				
			//카드입력칸 활성/비활성화
			$('input[type = radio][name = payway]').on('click', function() {
				
				var chkValue = $('input[type = radio][name = payway]:checked').val();
			
				if(chkValue == 'card') {
					$('#creditPay').css('display', 'block');
				} else if(chkValue != 'card')
					$('#creditPay').css('display', 'none');
			
			});
	});
	
	
	//전체 선택 및 해제
function check_all() {
	 // 전체 체크박스
	  const checkAll = document.getElementById("checkAll");

	  // 각 체크박스
	  const checkboxes = document.getElementsByName("chk");

	  // 전체 체크박스 체크되면
	  if (checkAll.checked) {
	    
		 // 각 체크박스 선택
	    for (let i = 0; i < checkboxes.length; i++) {
	      checkboxes[i].checked = true;
	    }
		 
	  } else {
	    // 모든 체크박스 선택 해제
	    for (let i = 0; i < checkboxes.length; i++) {
	      checkboxes[i].checked = false;
	    }
	  }
	}
	
	// 개별 체크박스 해제 시 전체박스 해제
	$(document).ready(function() {
	  var checkAll = document.getElementById("checkAll");
	  var checkboxes = document.getElementsByName("chk");

	  function onoffCheckItem() {
	    var allChecked = true;
	    for (var i = 0; i < checkboxes.length; i++) {
	      if (!checkboxes[i].checked) {
	        allChecked = false;
	        break;
	      }
	    }
	    checkAll.checked = allChecked;
	  }

	  for (var i = 0; i < checkboxes.length; i++) {d
		  checkboxes[i].addEventListener('click', onoffCheckItem);
	  };
	});

	

	
	function termChk() {
		var checkAll = $("#checkAll").is(":checked");
		var member_id = $("#member_id").val();
		var prd_id = $("#prd_id").val();
		var buy_quantity = $("#buy_quantity").val();
		var member_phone = $("#member_phone").val();
		var credit_number = $("#credit1").val()+"-"+$("#credit2").val()+"-"+$("#credit3").val()+"-"+$("#credit4").val();
		jQuery.ajaxSettings.traditional = true;
		
			
		var chkValue = $('input[type = radio][name = payway]:checked').val();
		
		if(member_id == 'admin') {
			swal("관리자는 돌아가세요", "관리자는 상품을 직접 구매할 수 없습니다.", "error")
			return false;
		} else if(member_id == "" || !member_id) {
			swal("회원 전용상품", "구매를 위해 로그인 해주시기 바랍니다.", "error")
			return false;
			
		} else if(chkValue == 'cash') {
			swal("결제 오류", "이 상품은 무통장입금을 지원하지 않습니다.", "error")
			return false;
			
		} else if ($("#credit1").val().length != 4 || $("#credit2").val().length != 4 || $("#credit3").val().length != 4 || $("#credit4").val().length != 4) {
			swal("입력 오류", "카드번호를 확인하세요", "error")
			return false;		
			
		} else if (checkAll != true) {
			swal("", "약관에 동의해야 합니다.", "error")
			return false;			
			
		} else if (checkAll == true && $("#credit1").val() != "" && $("#credit2").val() != "" && $("#credit3").val() != "" && $("#credit4").val() != "") {
				swal({
				icon : "info", 
				text : "결제를 진행 하시겠습니까?", 
				closeOnClickOutside : false, 
				closeOnEsc : false, 
				buttons : [ "취소", "결제" ],
			}).then(function(isConfirm) {
				if (isConfirm) {
					swal("결제 성공", "결제를 완료했습니다.", "success").then(function(isConfirm) {
						$.ajax({
							type : "POST", 
							url : "HotelPayment", 
							data : {
								"prd_id" : prd_id,
								"member_id" : member_id,
								"member_phone" : member_phone,
								"credit_number" : credit_number,
								"buy_quantity" : buy_quantity
							}, success : function(data) { window.location.href = "main"; }
						});
						console.log(prd_id);
					});
				}
			});
		}
	}

	$(document).ready(function() {				
		//카드입력칸 활성/비활성화
		$('input[type = radio][name = payway]').on('click', function() {
			
			var chkValue = $('input[type = radio][name = payway]:checked').val();
		
			if(chkValue == 'card') {
				$('#creditPay').css('display', 'block');
			} else if(chkValue != 'card')
				$('#creditPay').css('display', 'none');
		
		});
	});