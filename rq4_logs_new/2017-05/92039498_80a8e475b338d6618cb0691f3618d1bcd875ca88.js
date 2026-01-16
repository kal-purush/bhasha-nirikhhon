
$(function(){
	//子列表显示隐藏
	$(".main .conList li i").on("click",function(){
		if($(this).hasClass("fa-angle-right")){
			$(this).removeClass("fa-angle-right").addClass("fa-angle-down")
			.next().css("display","block");
		}else{
			$(this).removeClass("fa-angle-down").addClass("fa-angle-right")
			.next().css("display","none");
		}
	});
	
	getShopClass();
	
})

function getShopClass(){
	$.ajax({
		type:"get",
		url:"http://datainfo.duapp.com/shopdata/getclass.php",
		async:true,
		success:function(data){
			var dataArr = JSON.parse(data);
			var html="";
			$.each(dataArr,function(index){
				html+="<li>"+dataArr[index].className+"<i class='fa fa-angle-right'></i></li>";
				
				//跳转到商品列表页（这里暂时在本页显示商品）
				$(".list").delegate("li","touchstart",function(){
					window.location.href="index.html";
				});
			})
			$(".list").append(html);
		}
	});
}
