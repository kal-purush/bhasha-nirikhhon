$(document).ready(function(){
	
	    $(".addbtn").on('click',function(event){
			console.log($(".addbtn"));
			event.preventDefault();
			console.log("c");
				var item=$("#additem").val();
				var des=$("#Description").val();
				var p= $("#price").val();
				console.log(item);
				var abc= {item,des,p};
				console.log(abc);
			$.ajax({
				url:"/products",
				type:"post",
				data:abc,
				dataType:'json',
				
				success:function(data){	
				console.log(data);
					$(".newitem").append('<div class="newbox well well-lg">'+'<span class="sign glyphicon glyphicon-remove-sign pull-right">'+'</span>'+'<button class="pricebtn btn btn-primary pull-right"  type="button">'+data.p+'</button>'+'<h2>'+'<strong>'+data.item+'</strong>'+'</h2>'+'<p>'+'<strong>'+data.des+'</strong>'+'</p>'+'</div>');
						
				}
			})
		})
});