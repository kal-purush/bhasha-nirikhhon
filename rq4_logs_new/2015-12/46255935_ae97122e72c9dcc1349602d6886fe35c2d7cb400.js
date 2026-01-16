$(function(){
	
	$(".chaxun").button({
		disabled:false,//禁用按钮
		icons:{//button按钮才有效
			primary:"ui-icon-folder-open",
			secondary: "ui-icon-triangle-1-s"
		},
		label:"搜索",
		text:true,//设置为false时不显示文本，但必须设置icons选项	
		/*create:function(event,ui){
			alert("创建");
		},*/
	});
	
	//alert($(".chaxun").button("option","disabled"));	
	//alert($(".chaxun").button("option","label",'改变值'));

	$(".tiwen").button();
	$(".tiwen").button("disable");//禁用button，不接受其他参数
	$(".tiwen").button('enable');//启用button不接受其他参数
	$(".tiwen").button("refresh");//刷新按钮的视觉状态。用于在以编程方式改变原生元素的选中状态或禁用状态后更新按钮状态
	
	
	//$(".reg").buttonset();
	//$(".reg input[type=radio]").button();
	//单选框按钮，上述两种方式都可
	
	//$(".reg").buttonset();
	$(".reg input[name=color]").button();
	//多选框按钮，上述两种方式都可	


	$(".reg_a").click(function(){
		$(".reg").dialog({
			title:"用户注册",
			buttons:{ //提交按钮
				'提交':function(){
					alert("正在ajax提交");	
				},
				'取消':function(){
					$(this).dialog("close");
				}
			},
			//position:["right","top"],//打开的位置
			//modal:true,//遮罩层
			//autoOpen:false,//是否自动显示对话框，false是需要使用open方法显示对话框
			resizable:true,//是否允许调整大小
			maxHeight:600,
			maxWidth:600,
			show:"puff",//打开效果fold ,puff,slide
			hide:"slide",//关闭效果
			closeOnEscape:true,//是否在按esc时关闭对话框（需要有输入焦点）
			dialogClass:"",//添加额外对话框css Class
			draggable:true,//设定对话框是否可以拖动
			height:250,//对话框高度
			width:300,//对话框高度
			stack:true,//是否可覆盖其他对话框
			zIndex:50,
			autoOpen:true,//需要使用open方法打开
			//focus:function(event,ui){
			//	alert("得到焦点");
			//},
			
			/*resize:function(event,ui){
				alert("对话框大小改变，促发此事件");
			},
			resizeStart:function(event,ui){
				alert('当开始改变对话框大小时，促发此事件');
				alert(ui.originalSize.width+'---'+ui.originalSize.height);//调整之前
				alert(ui.size.width+'---'+ui.size.height);//当前
				alert(ui.originalPosition.top+'---'+ui.originalPosition.left);//调整之前
				alert(ui.position.top+'---'+ui.position.left);//当前
			},
			
			resizeStop:function(event,ui){
				alert("当对话框大小改变后，促发此事件");
				alert(ui.originalSize.width+'---'+ui.originalSize.height);
				alert(ui.size.width+'---'+ui.size.height);
				alert(ui.originalPosition.top+'---'+ui.originalPosition.left);
				alert(ui.position.top+'---'+ui.position.left);
			},*/
			
			/*drag:function(event,ui){
				alert("每次移动对话框执行");
				alert("距离上边距"+ui.position.top+",移动了"+ui.offset.top);
			},
			dragStart:function(event,ui){
				alert("开始移动时执行");
				alert(ui.position.top+'--'+ui.position.left);
			},
			dragStop:function(event,ui){
				alert("停止移动时执行");
				alert(ui.position.top+'--'+ui.position.left);
			},*/
		});
		//alert($('.reg').parent().find('button').length);//查找对话框中的button按钮
		//$(".reg").parent().find('button').eq(1).button('disable');
		$(".reg").parent().find('button').eq(2).button({
			icons:{
				primary:"ui-icon-close",
			},
			label:"取消",
			text:true,
		});
	});
	
	
	/*$(".reg_a").click(function(){
		$(".reg").dialog("open");
		alert($(".reg").dialog('option','title'));
		$(".reg").dialog('option','title','哈哈哈');
	});*/
	
	
	
	$(".login_a").click(function(){
		$(".login").dialog({
			focus:function(event,ui){
				alert("获取焦点");
			},
		});
	});
	
	
	
	
	
	
	
	
});