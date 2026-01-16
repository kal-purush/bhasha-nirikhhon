// JavaScript Document

function addadmin() {
	layui.use('layer', function() {
		var layer = layui.layer;
		
		var username = $('#username').val();
		var password = $('#password').val();
		var name = $('#name').val();
		
		if (username == "" || password == "" || name == "") {
			layer.msg('用户名密码姓名不能为空');
		} else {
		
			layer.load(0, {shade: false});

			$.post('../api/admin.jsp?type=addadmin', {
				username: username,
				password: password,
				name: name
			}, function(data) {
				layer.closeAll();
				if (data.indexOf("success") >= 0) {
					layer.msg('添加成功', {icon: 1});
				} else if (data.indexOf("null") >= 0) {
					layer.msg('用户名密码姓名不能为空');
				} else if (data.indexOf("error") >= 0) {
					layer.msg('添加失败', {icon: 2});
				} else {
					layer.msg('未知错误');
				}
			});
		}
	});
}