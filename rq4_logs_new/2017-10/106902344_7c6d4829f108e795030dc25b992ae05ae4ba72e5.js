var user_id;
user_id=getUrlParam("user_id");
//alert("user_id"+user_id)
$(document).ready(function(){
	 jQuery.jqtab = function(tabtit,tabcon) {
	        $(tabcon).hide();
	        $(tabtit+" li:first").addClass("thistab").show();
	        $(tabcon+":first").show();
	    
	        $(tabtit+" li").click(function() {
	            $(tabtit+" li").removeClass("thistab");
	            $(this).addClass("thistab");
	            $(tabcon).hide();
	            var activeTab = $(this).find("a").attr("tab");
	            $("#"+activeTab).fadeIn();
	            return false;
	        });
	        
	    };
	    /*调用方法如下：*/
	    $.jqtab("#tabs",".tab_con");
	    
 // showAuditingList("1"); //显示待审核的列表
  showAuditingList1("1") //显示已审核的列表
 //alert(1111111);
  showPage();
})
//批量删除
function dataOrderdel(){
	//单选
	
	
	/*批量删除*/
	//判断是否至少选择一项
	var checkedNum = $("input[name='subChk']:checked").length;
	if(checkedNum == 0){
		alert("至少选择一项");
		return;
	}
	//批量选择
	if(confirm("确定要删除所有的选项")){
		var checkedList = new Array();		
		$("input[name='subChk']:checked").each(function(){
			checkedList.push($(this).val());
		});
	$.ajax({
		type:"POST",
		url:"./order.jhtm?method=DeleteOrderList",
		data:{"order_list":checkedList.toString()},
		dataType: "json",
		success:function(json){
			alert("删除成功");
			//window.location.reload();
			setTimeout("location.reload();",1000);

		}
	})
}
}
function showAuditingList(page){
	$.ajax({
		type: "post",
		url: "./order.jhtm?method=GetReviewOrderList",
		data:{page:page},
		dataType: "json",
		success: function(json){
			console.log(json);
			var num = json.totalnum;
			$(".ty_data").html("");
			var state = "";                  //状态文字
			var spanColor = "";              //状态颜色
			var operation = "";              //操作
            $(".text-c").html("");
            $(".text-c").append('<th width="80" class="hidden-xs">订单号</th>'
						+'<th width="100" class="hidden-xs">客户名称</th>'
						+'<th width="100">接单时间</th>'
						+'<th width="90">交货时间</th>'
						+'<th width="130">创建时间</th>'
						+'<th width="70">进度</th>'
						+'<th width="100" id="cao">操作</th>')
			for(var i=0;i<json.result.length;i++){
				
				var edittemp = "修改状态";
				var editurl = "Admin/Auditing/auditing-edit.html?order_id="+json.result[i].order_id+"&"+json.result[i].schedule+"&user_id="+user_id+"";
				var showtemp= "订单详情";
				var showurl = "Admin/Order/order-detail.jsp?order_id="+json.result[i].order_id+"";		
				//if( json.result[i].schedule>=2 && json.result[i].schedule<num){
//					
//					state = "待审核";
//					spanColor = "label label-success radius";
					operation = '<td class="td-manage"><a title="审核" href="javascript:;" onclick="order_change(this,'+json.result[i].order_id+','+json.result[i].schedule+')" class="ml-5" style="text-decoration:none"><i class="Hui-iconfont">&#xe6df;</i></a><a title="修改状态" href="javascript:;" onclick="order_edit('+'\''+edittemp+'\''+','+'\''+editurl+'\''+',500,210)" class="ml-5" style="text-decoration:none"><i class="Hui-iconfont">&#xe627;</i></a></td>';
//				}
				$(".tr_data").append(
					'<tr class="text-c">'
						//+'<th width="25"><input type="checkbox" name="subChk" value=""></th>'
						+'<td><u style="cursor:pointer" class="text-primary" onclick="orderdetail_show('+'\''+showtemp+'\''+','+'\''+showurl+'\''+',4,510)">'+json.result[i].order_id+'</u></td>'
						+'<td>'+json.result[i].client_name+'</td>'
						+'<td>'+json.result[i].order_time+'</td>'
						+'<td>'+json.result[i].delivery_time+'</td>'
						+'<td>'+json.result[i].end_time+'</td>'
						+'<td class="td-status"><span class="'+spanColor+'">待审核</span></td>'
						+''+operation+''
					+'</tr>')
			}
		}
	})
}
function showAuditingList1(page) {
	$.ajax({
		type: "post",
		url: "./order.jhtm?method=GetReviewEdOrderList",
		data:{page:page},
		dataType: "json",
		success: function(json){
			//alert(1111111111111111111);
			//alert(json);
			
			var state = "";                  //状态文字
			var spanColor="";
			var operation = "";              //操作
//             $(".tr_data_ty").html("");
//             $(".text-c-b").html("");
//             $(".text-c-b").append('<th width="80" class="hidden-xs">订单号</th>'
// 						+'<th width="100" class="hidden-xs">客户名称</th>'
// 						+'<th width="100">接单时间</th>'
// 						+'<th width="90">交货时间</th>'
// 						+'<th width="130">创建时间</th>'
// 						+'<th width="70">进度</th>')
			for(var i=0;i<json.result.length;i++){
				
				//var edittemp = "修改进度";
				//var editurl = "Admin/Auditing/auditing-edit.html?order_id="+json.result[i].order_id+"";
				//var schedul = "修改进度";
				//var schedulurl = ""
				var showtemp= "订单详情";
				var showurl = "Admin/Order/order-detail.jsp?order_id="+json.result[i].order_id+"&order_name="+json.result[i].order_name+"&schedule="+json.result[i].schedule+"";	
				if( json.result[i].schedule = 2){
//					//$("#cao").hidden();
//					//document.getElementById("cao").style.display="none";
					state = "已审核";
					spanColor = "label label-defaunt radius";
					operation = '<td class="td-manage"> <a title="删除" href="javascript:;" id="kk" onclick="order_del('+json.result[i].order_id+')" class="ml-5" style="text-decoration:none"><i class="Hui-iconfont">&#xe6e2;</i></a></td>';
				}
				$(".tr_data").append('<tr class="text-c">'
						+'<th width="25"><input type="checkbox" name="subChk" value=""></th>'
						+'<td><u style="cursor:pointer" class="text-primary" onclick="orderdetail_show('+'\''+showtemp+'\''+','+'\''+showurl+'\''+',4,510)">'+json.result[i].order_name+'</u></td>'
						+'<td>'+json.result[i].client_name+'</td>'
						+'<td>'+json.result[i].order_time+'</td>'
						+'<td>'+json.result[i].delivery_time+'</td>'
						+'<td>'+json.result[i].end_time+'</td>'
						+'<td class="td-status"><span class="'+spanColor+'">已审核</span></td>'
						+''+operation+''
					+'</tr>');
			}
		}
	})
}
//删除订单状态
function order_del(order_id){
	
	$.ajax({
		type: "post",
		url: "./order.jhtm?method=DeleteOrder",
		data:{orderid:order_id},
		dataType: "json",
		success: function(json){
			alert("删除成功");
			window.location.reload();
			//setTimeout("location.reload();",1000);
		}
	})
}

judge=1;
//审核订单
function changeSchedule(order_id,schedule){
	$.getJSON("http://localhost:8080/pdf_2017-07-07/order.jhtm", {
		method: "ReviewOrder",
		order_id:order_id,
		schedule:schedule,
		user_id:user_id,
		judge:1
	}, function(json){
		 //json=result;
		   alert("审核成功");
		});
}
function getUrlParam(name) {
	var reg = new RegExp("(^|&)" + name + "=([^&]*)(&|$)"); // 构造一个含有目标参数的正则表达式对象
	var r = window.location.search.substr(1).match(reg);
	if (r != null)
		return unescape(r[2]);
	return null; // 返回参数值
}
var pageNow = 1;	//显示当前页码
function showPage(){
	
	$.ajaxSetup({                    //设置同步
	    async : false  
	}); 
	
	var pageAll = 0;
	var order_id = $("#orderId").val();                //获取订单号
	
	$.post("./order.jhtm", {
		method : "getReviewNumber",
	}, function(json) {
		//alert(json);
		document.getElementById("all_data").innerHTML = json;          //总数据
		pageAll = Math.ceil(json/10);               //页数
	});
	
	
	layui.config({
		base: './Admin/Mode/plugins/layui/modules/'
	}); 

	layui.use(['icheck', 'laypage','layer'], function() {
		var $ = layui.jquery,
			laypage = layui.laypage,
			layer = parent.layer === undefined ? layui.layer : parent.layer;
		$('input').iCheck({
			checkboxClass: 'icheckbox_flat-green'
	});

	//page
	laypage({
		cont: 'page',
		pages: pageAll //总页数
			,
		groups: 5 //连续显示分页数
			,
		jump: function(obj, first) {
			//得到了当前页，用于向服务端请求对应数据
			var curr = obj.curr;
			if(!first) {
				GetReviewOrderList(curr)     //跳转
			}
		}
	});
	});
}