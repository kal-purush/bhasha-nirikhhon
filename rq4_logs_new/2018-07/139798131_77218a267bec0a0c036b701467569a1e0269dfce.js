var role_auth = function() {

	var handleTable = function(roledata) {
		var setgrid = new Datatable();
		var url = Sunline.ajaxPath("auth/allroleauth");
		var editUrl;
		var table = $("#role_auth_ajax");
		var setAuthform = $("#setAuthModal");
		var authTypeDict = Sunline.getDict("authType");
		var authDict = Sunline.getDict(roledata.authType, "/auth", "authCd",
				"menuName");
		$("#Auth_authCd").select2({
			data : authDict
		});
		/*
		 * 初始化table
		 */
		if (!Sunline.isNull(roledata.registerCd)) {
			setgrid.setAjaxParam('registerCd', roledata.registerCd);
		}
		if (!Sunline.isNull(roledata.authType)) {
			setgrid.setAjaxParam('authType', roledata.authType);
		}
		if (!Sunline.isNull(roledata.roleCd)) {
			setgrid.setAjaxParam('roleCd', roledata.roleCd);
		}
		setgrid.init({
					src : table,
					deleteData : sendData,
					onSuccess : function(setgrid) {
					},
					onError : function(setgrid) {
					},
					dataTable : { // here you can define a typical
						"ajax" : {
							"url" : url, // ajax source
						},
						"bDestroy" : true,
						"bServerSide" : true,
						"columns" : [
								{
									"data" : "registerCd",
									"sortable" : false,
									"searchable" : false
								},
								{
									"data" : "authType",
									"sortable" : false,
									"searchable" : false,
									"render" : function(data, type, full) {
										for (var i = 0; i < authTypeDict.length; i++) {
											if (authTypeDict[i].id == data) {
												return authTypeDict[i].text;
											}
										}
										return data;
									}
								},
								{
									"data" : "roleCd",
									"sortable" : false,
									"searchable" : false
								},
								{
									"data" : "authCd",
									"sortable" : false,
									"searchable" : false,
									"render" : function(data, type, full) {
										for (var i = 0; i < authDict.length; i++) {
											if (authDict[i].id == data) {
												return authDict[i].text;
											}
										}
										return data;
									}
								},
								{
									"data" : null,
									"sortable" : false,
									"searchable" : false,
									"render" : function(data, type, full) {
										return "<a class='delete' href='javascript:;' data-src='"
												+ data.registerCd
												+ ","
												+ data.authType
												+ ","
												+ data.roleCd
												+ ","
												+ data.authCd + "'>删除 </a>";
									}
								} ]
					}
				});

		var sendData = [ "registerCd", "authType", "roleCd", "authCd" ];
		// 绑定删除事件
		setgrid.bindTableDelete(sendData);
		
		//下拉框回收
		$("#setAuthModal").click(function(){
		   	  $(".select-close-1").select2("close");
		     });
		
		// 新增窗口
		$("#add_Auth_btn", $("#add_btn_set")).bind("click", function() {
			$('#Auth_registerCd', setAuthform).val(roledata.registerCd);
			$('#Auth_authType', setAuthform).val(roledata.authType);
			$('#Auth_roleCd', setAuthform).val(roledata.roleCd);
			editUrl = "auth/addRoleAuth";
			setAuthform.modal('show');
			setAuthform.on("hide.bs.modal", function() {
				$(".alert-success", $('form', setAuthform)).hide();
				$('.alert-danger', $('form', setAuthform)).hide();
				$(".msg", $('form', setAuthform)).text("");
				setgrid.submitFilter();
			});
			return false;
		});

		$("#subAuth_btn").click(function() {
			$("#edit_auth_form").submit();
		});

		/*
		 * 表单验证方法
		 */
		var roleValid = Sunline.getValidate($("form", setAuthform), function(
				form) {
			var data = {};
			$.each($("input", setAuthform), function(i, n) {
				data[n.name] = n.value;
			});

			Sunline.ajaxRouter(editUrl, data, "post", function(data, status) {
				$('.msg', setAuthform).text(data.msg);
				if (data.ret == "success") {
					$('.alert-success', setAuthform).show();
					$('.alert-danger', setAuthform).hide();
					$('#registerCd', setAuthform).attr("readOnly", true);
					$('#authType', setAuthform).attr("readOnly", true);
					$('#roleCd', setAuthform).attr("readOnly", true);
				} else {
					$('.alert-success', setAuthform).hide();
					$('.alert-danger', setAuthform).show();
				}
			}, function() {
				$('.msg', setAuthform).text("请求出错!");
				$('.alert-success', setAuthform).hide();
				$('.alert-danger', setAuthform).show();
			}, "json");
		}, { // 验证规则
			registerCd : {
				required : true,
				rangelength : [ 2, 19 ]
			},
			authType : {
				required : true,
				maxlength : 1,
				number : true
			},
			roleCd : {
				required : true,
				rangelength : [ 2, 19 ]
			},
			authCd : {
				required : true,
				rangelength : [ 2, 19 ]
			}
		});
	}

	return {
		init : function(data) {
			handleTable(data);
		}
	}
}();