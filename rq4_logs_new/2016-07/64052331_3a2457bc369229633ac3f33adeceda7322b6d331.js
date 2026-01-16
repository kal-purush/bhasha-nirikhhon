//微信二维码
$(".log-wechat").mouseover(function(){
	$(".blank",".log-wechat").css("display","block");
	$(".erweima",".log-wechat").css("display","block");
}).mouseout(function(){
	$(".blank",".log-wechat").css("display","none");
	$(".erweima",".log-wechat").css("display","none");
})
//手机
$(".log-phone").mouseover(function(){
	$(".blank",".log-phone").css("display","block");
	$(".erweima",".log-phone").css("display","block");
}).mouseout(function(){
	$(".blank",".log-phone").css("display","none");
	$(".erweima",".log-phone").css("display","none");
})
//我的银泰
$(".yt-hom").mouseover(function(){
	$(".blank",".yt-hom").css("display","block");
	$(".yt-hm",".yt-hom").css("display","block");
}).mouseout(function(){
	$(".blank",".yt-hom").css("display","none");
	$(".yt-hm",".yt-hom").css("display","none");
})
//购物袋
$(".gouwudaibox").mouseover(function(){
	$(".gwd-box",".gouwudaibox").css("display","block");
}).mouseout(function(){
	$(".gwd-box",".gouwudaibox").css("display","none");
})

//search
var search=$(".search-left",".search")[0];
	search.onfocus=function(){
		if(this.value=="塑形丝袜满赠推荐"){
			this.value="";
		}
	}
	search.onblur=function(){
		if(this.value==""){
			this.value="塑形丝袜满赠推荐";
		}
	}


// menu
$("dl",".menu").mouseover(function(){
	var index=$(this).index("dl",".menu");	
	$("dt",".menu").removeClass("selected").eq(index).addClass("selected");
	$("dd",".menu").css({display:"none"}).eq(index).css({display:"block"});
}).mouseout(function(){
	$("dt",".menu").removeClass("selected");
	$("dd",".menu").css({display:"none"});
})

// banner

$(".banner-img",".main-banner").fadeOut(0).eq(0).fadeIn(0);
$("li",".banner-button").removeClass("hotbutton").eq(0).addClass("hotbutton");
var num=0;
function move(type){
	if(type=="r"){
		num++;
		if(num>=$(".banner-img",".main-banner").length){
			num=0;
		}
	}else if(type=="l"){
		num--;
		if(num<=-1){
			num=$(".banner-img",".main-banner").length-1;
		}
	}
	$(".banner-img",".main-banner").fadeOut(500).eq(num).fadeIn(1000);
	$("li",".banner-button").removeClass("hotbutton").eq(num).addClass("hotbutton");
}
var t=setInterval(function(){
		move("r");
	},2000);
$(".banner",".bannerbox").hover(function(){
	clearInterval(t);
	$(".banner>a").css({display:"block"});
},function(){
	t=setInterval(function(){
		move("r");
	},2000);
	$(".banner>a").css({display:"none"});
})
$(".banner-prev",".banner").click(function(){
	move("l");
});
$(".banner-next",".banner").click(function(){
	move("r");
});

$("li",".bannerbutton").mouseover(function(){
	var ban=$(this).index();
	// alert(ban);
	$(".banner-img",".main-banner").stop(true,true);
	$(".banner-img",".main-banner").fadeOut(500).eq(ban).fadeIn(1000);
	$("li",".banner-button").removeClass("hotbutton").eq(ban).addClass("hotbutton");
	num=ban;	
})

// banner-bottom1
$("li",".bottom1-menu").removeClass("menu-active").eq(2).addClass("menu-active");
$("span",".bottom1-menu").css({display:"none"}).eq(2).css({display:"block"});
$(".menuimg-box",".bottom1-menuimg").css({display:"none"}).eq(2).css({display:"block"});
$("li",".bottom1-menu").mouseover(function(){
	var bot=$(this).index();
	$("li",".bottom1-menu").removeClass("menu-active").eq(bot).addClass("menu-active");
	$("span",".bottom1-menu").css({display:"none"}).eq(bot).css({display:"block"});
	$(".menuimg-box",".bottom1-menuimg").css({display:"none"}).eq(bot).css({display:"block"});

})

//zgtk
$("li",".zgtkrighttop-nav").removeClass("zgtk-active").eq(0).addClass("zgtk-active");
$("span",".zgtkrighttop-nav").css({display:"none"}).eq(0).css({display:"block"});
$(".zgtkright-main>div").css({display:"none"}).eq(0).css({display:"block"});
$("li",".zgtkrighttop-nav").mouseover(function(){
	var zgtk=$(this).index();
	$("li",".zgtkrighttop-nav").removeClass("zgtk-active").eq(zgtk).addClass("zgtk-active");
	$("span",".zgtkrighttop-nav").css({display:"none"}).eq(zgtk).css({display:"block"});
	$(".zgtkright-main>div").css({display:"none"}).eq(zgtk).css({display:"block"});

})

// 楼层跳转
$(window).scroll(function(){
	var tops=$(window).scrollTop();
	if(tops>=($(".banner-bottom1").position().top+180)){
		$(".mainfloor").fadeIn(400);
	}else{
		$(".mainfloor").fadeOut(400);
	}
	$(".floor-yt").each(function(index,nowobj){
		if(tops>=$(nowobj).position().top){
			$(".mainfloor>.a1").removeClass("hov").eq(index).addClass("hov");
		}
	})

	$(".mainfloor>.a1").click(function(){
 
		var flo=$(this).index(".mainfloor>.a1")+1;
		var PT=$(".floor-yt").eq(flo).position().top;
		// var position=$(".floor-yt").eq(flo).position();
		// var PT=$(position).top;
		/*$(window).scrollTop(PT);*/
		var obj={st:0}
		$(obj).animate({st:PT},{
			speed:1000,
			step:function(){
			$(window).scrollTop(obj.st);
			}
		})

		$(obj).animate({scrollTop:PT},600);
		$(".mainfloor>a").removeClass("hov").eq(flo).addClass("hov");
	})


	$(".foot1").each(function(index,nowobj){
		// alert(1);
		if($(nowobj).position().top<=(tops+$(window).height())){
			var imgs=$("img",nowobj);
			$(imgs).each(function(index1,nowobj1){
				var src=$(nowobj1).attr("data-src");
				$(nowobj1).attr("src",src);
			})
		}
	})
	// 返回顶部
	$(".yt-floor-top").click(function(){
		var obj1={st:tops};
		$(obj1).animate({st:0},{
			speed:1000,
			step:function(){
			$(window).scrollTop(obj1.st);
			}
		})
	})
});

// 名品栏目左下角
$(".floor-prev",".floor-mp").click(function(){
	$(".floornavbottom-imgbox",".floor-mp").animate({left:"-170px"},function(){
		$(".floornavbottom-img",".floor-mp").first().insertAfter($(".floornavbottom-img",".floor-mp").last());
		$(".floornavbottom-imgbox",".floor-mp").css({left:"0"});
	});		
})
$(".floor-next",".floor-mp").click(function(){
	$(".floornavbottom-img",".floor-mp").last().insertBefore($(".floornavbottom-img",".floor-mp").first());
	$(".floornavbottom-imgbox",".floor-mp").css({left:"-170px"});
	$(".floornavbottom-imgbox",".floor-mp").animate({left:"0"},function(){
	});	
});	

// 女装栏目左下角
$(".floor-prev",".floor-nz").click(function(){
	$(".floornavbottom-imgbox",".floor-nz").animate({left:"-170px"},function(){
		$(".floornavbottom-img",".floor-nz").first().insertAfter($(".floornavbottom-img",".floor-nz").last());
		$(".floornavbottom-imgbox",".floor-nz").css({left:"0"});
	});		
})
$(".floor-next",".floor-nz").click(function(){
	$(".floornavbottom-img",".floor-nz").last().insertBefore($(".floornavbottom-img",".floor-nz").first());
	$(".floornavbottom-imgbox",".floor-nz").css({left:"-170px"});
	$(".floornavbottom-imgbox",".floor-nz").animate({left:"0"},function(){
	});	
});	

// 男装栏目左下角
$(".floor-prev",".floor-manz").click(function(){
	$(".floornavbottom-imgbox",".floor-manz").animate({left:"-170px"},function(){
		$(".floornavbottom-img",".floor-manz").first().insertAfter($(".floornavbottom-img",".floor-manz").last());
		$(".floornavbottom-imgbox",".floor-manz").css({left:"0"});
	});		
})
$(".floor-next",".floor-manz").click(function(){
	$(".floornavbottom-img",".floor-manz").last().insertBefore($(".floornavbottom-img",".floor-manz").first());
	$(".floornavbottom-imgbox",".floor-manz").css({left:"-170px"});
	$(".floornavbottom-imgbox",".floor-manz").animate({left:"0"},function(){
	});	
});	

// 鞋靴栏目左下角
$(".floor-prev",".floor-xx").click(function(){
	$(".floornavbottom-imgbox",".floor-xx").animate({left:"-170px"},function(){
		$(".floornavbottom-img",".floor-xx").first().insertAfter($(".floornavbottom-img",".floor-xx").last());
		$(".floornavbottom-imgbox",".floor-xx").css({left:"0"});
	});		
})
$(".floor-next",".floor-xx").click(function(){
	$(".floornavbottom-img",".floor-xx").last().insertBefore($(".floornavbottom-img",".floor-xx").first());
	$(".floornavbottom-imgbox",".floor-xx").css({left:"-170px"});
	$(".floornavbottom-imgbox",".floor-xx").animate({left:"0"},function(){
	});	
});	

// 箱包栏目左下角
$(".floor-prev",".floor-xb").click(function(){
	$(".floornavbottom-imgbox",".floor-xb").animate({left:"-170px"},function(){
		$(".floornavbottom-img",".floor-xb").first().insertAfter($(".floornavbottom-img",".floor-xb").last());
		$(".floornavbottom-imgbox",".floor-xb").css({left:"0"});
	});		
})
$(".floor-next",".floor-xb").click(function(){
	$(".floornavbottom-img",".floor-xb").last().insertBefore($(".floornavbottom-img",".floor-xb").first());
	$(".floornavbottom-imgbox",".floor-xb").css({left:"-170px"});
	$(".floornavbottom-imgbox",".floor-xb").animate({left:"0"},function(){
	});	
});	

// 美容栏目左下角
$(".floor-prev",".floor-mr").click(function(){
	$(".floornavbottom-imgbox",".floor-mr").animate({left:"-170px"},function(){
		$(".floornavbottom-img",".floor-mr").first().insertAfter($(".floornavbottom-img",".floor-mr").last());
		$(".floornavbottom-imgbox",".floor-mr").css({left:"0"});
	});		
})
$(".floor-next",".floor-mr").click(function(){
	$(".floornavbottom-img",".floor-mr").last().insertBefore($(".floornavbottom-img",".floor-mr").first());
	$(".floornavbottom-imgbox",".floor-mr").css({left:"-170px"});
	$(".floornavbottom-imgbox",".floor-mr").animate({left:"0"},function(){
	});	
});	

// 运动栏目左下角
$(".floor-prev",".floor-yd").click(function(){
	$(".floornavbottom-imgbox",".floor-yd").animate({left:"-170px"},function(){
		$(".floornavbottom-img",".floor-yd").first().insertAfter($(".floornavbottom-img",".floor-yd").last());
		$(".floornavbottom-imgbox",".floor-yd").css({left:"0"});
	});		
})
$(".floor-next",".floor-yd").click(function(){
	$(".floornavbottom-img",".floor-yd").last().insertBefore($(".floornavbottom-img",".floor-yd").first());
	$(".floornavbottom-imgbox",".floor-yd").css({left:"-170px"});
	$(".floornavbottom-imgbox",".floor-yd").animate({left:"0"},function(){
	});	
});	

// 配饰栏目左下角
$(".floor-prev",".floor-ps").click(function(){
	$(".floornavbottom-imgbox",".floor-ps").animate({left:"-170px"},function(){
		$(".floornavbottom-img",".floor-ps").first().insertAfter($(".floornavbottom-img",".floor-ps").last());
		$(".floornavbottom-imgbox",".floor-ps").css({left:"0"});
	});		
})
$(".floor-next",".floor-ps").click(function(){
	$(".floornavbottom-img",".floor-ps").last().insertBefore($(".floornavbottom-img",".floor-ps").first());
	$(".floornavbottom-imgbox",".floor-ps").css({left:"-170px"});
	$(".floornavbottom-imgbox",".floor-ps").animate({left:"0"},function(){
	});	
});	

// 家居栏目左下角
$(".floor-prev",".floor-jj").click(function(){
	$(".floornavbottom-imgbox",".floor-jj").animate({left:"-170px"},function(){
		$(".floornavbottom-img",".floor-jj").first().insertAfter($(".floornavbottom-img",".floor-jj").last());
		$(".floornavbottom-imgbox",".floor-jj").css({left:"0"});
	});		
})
$(".floor-next",".floor-jj").click(function(){
	$(".floornavbottom-img",".floor-jj").last().insertBefore($(".floornavbottom-img",".floor-jj").first());
	$(".floornavbottom-imgbox",".floor-jj").css({left:"-170px"});
	$(".floornavbottom-imgbox",".floor-jj").animate({left:"0"},function(){
	});	
});	


// 名品栏目中间
$(".floor-centerbox>div",".floor-mp").hover(function(){
	$(".floorcenter-prev",".floor-mp").animate({left:"0"});
	$(".floorcenter-next",".floor-mp").animate({right:"0"});
},function(){
	$(".floorcenter-prev",".floor-mp").animate({left:"-30px"});
	$(".floorcenter-next",".floor-mp").animate({right:"-30px"});
})
$(".floorcenter-prev",".floor-mp").click(function(){
	$(".floorcenter-main-imgbox",".floor-mp").animate({left:"0"});
	$(".floorcenter-button>li",".floor-mp").removeClass("floor-active").eq(0).addClass("floor-active");
})
$(".floorcenter-next",".floor-mp").click(function(){
	$(".floorcenter-main-imgbox",".floor-mp").animate({left:"-390px"});
	$(".floorcenter-button>li",".floor-mp").removeClass("floor-active").eq(1).addClass("floor-active");
})

$(".floorcenter-button>li",".floor-mp").eq(0).click(function(){
	$(".floorcenter-main-imgbox",".floor-mp").animate({left:"0"});
	$(".floorcenter-button>li",".floor-mp").removeClass("floor-active").eq(0).addClass("floor-active");
})
$(".floorcenter-button>li",".floor-mp").eq(1).click(function(){
	$(".floorcenter-main-imgbox",".floor-mp").animate({left:"-390px"});
	$(".floorcenter-button>li",".floor-mp").removeClass("floor-active").eq(1).addClass("floor-active");
})


// 鞋靴栏目中间
$(".floor-centerbox>div",".floor-xx").hover(function(){
	$(".floorcenter-prev",".floor-xx").animate({left:"0"});
	$(".floorcenter-next",".floor-xx").animate({right:"0"});
},function(){
	$(".floorcenter-prev",".floor-xx").animate({left:"-30px"});
	$(".floorcenter-next",".floor-xx").animate({right:"-30px"});
})
$(".floorcenter-prev",".floor-xx").click(function(){
	$(".floorcenter-main-imgbox",".floor-xx").animate({left:"0"});
	$(".floorcenter-button>li",".floor-xx").removeClass("floor-active").eq(0).addClass("floor-active");
})
$(".floorcenter-next",".floor-xx").click(function(){
	$(".floorcenter-main-imgbox",".floor-xx").animate({left:"-390px"});
	$(".floorcenter-button>li",".floor-xx").removeClass("floor-active").eq(1).addClass("floor-active");
})
$(".floorcenter-button>li",".floor-xx").eq(0).click(function(){
	$(".floorcenter-main-imgbox",".floor-xx").animate({left:"0"});
	$(".floorcenter-button>li",".floor-xx").removeClass("floor-active").eq(0).addClass("floor-active");
})
$(".floorcenter-button>li",".floor-xx").eq(1).click(function(){
	$(".floorcenter-main-imgbox",".floor-xx").animate({left:"-390px"});
	$(".floorcenter-button>li",".floor-xx").removeClass("floor-active").eq(1).addClass("floor-active");
})


// 箱包栏目中间
$(".floor-centerbox>div",".floor-xb").hover(function(){
	$(".floorcenter-prev",".floor-xb").animate({left:"0"});
	$(".floorcenter-next",".floor-xb").animate({right:"0"});
},function(){
	$(".floorcenter-prev",".floor-xb").animate({left:"-30px"});
	$(".floorcenter-next",".floor-xb").animate({right:"-30px"});
})
$(".floorcenter-prev",".floor-xb").click(function(){
	$(".floorcenter-main-imgbox",".floor-xb").animate({left:"0"});
	$(".floorcenter-button>li",".floor-xb").removeClass("floor-active").eq(0).addClass("floor-active");
})
$(".floorcenter-next",".floor-xb").click(function(){
	$(".floorcenter-main-imgbox",".floor-xb").animate({left:"-390px"});
	$(".floorcenter-button>li",".floor-xb").removeClass("floor-active").eq(1).addClass("floor-active");
})
$(".floorcenter-button>li",".floor-xb").eq(0).click(function(){
	$(".floorcenter-main-imgbox",".floor-xb").animate({left:"0"});
	$(".floorcenter-button>li",".floor-xb").removeClass("floor-active").eq(0).addClass("floor-active");
})
$(".floorcenter-button>li",".floor-xb").eq(1).click(function(){
	$(".floorcenter-main-imgbox",".floor-xb").animate({left:"-390px"});
	$(".floorcenter-button>li",".floor-xb").removeClass("floor-active").eq(1).addClass("floor-active");
})


// 美容栏目中间
$(".floor-centerbox>div",".floor-mr").hover(function(){
	$(".floorcenter-prev",".floor-mr").animate({left:"0"});
	$(".floorcenter-next",".floor-mr").animate({right:"0"});
},function(){
	$(".floorcenter-prev",".floor-mr").animate({left:"-30px"});
	$(".floorcenter-next",".floor-mr").animate({right:"-30px"});
})
$(".floorcenter-prev",".floor-mr").click(function(){
	$(".floorcenter-main-imgbox",".floor-mr").animate({left:"0"});
	$(".floorcenter-button>li",".floor-mr").removeClass("floor-active").eq(0).addClass("floor-active");
})
$(".floorcenter-next",".floor-mr").click(function(){
	$(".floorcenter-main-imgbox",".floor-mr").animate({left:"-390px"});
	$(".floorcenter-button>li",".floor-mr").removeClass("floor-active").eq(1).addClass("floor-active");
})
$(".floorcenter-button>li",".floor-mr").eq(0).click(function(){
	$(".floorcenter-main-imgbox",".floor-mr").animate({left:"0"});
	$(".floorcenter-button>li",".floor-mr").removeClass("floor-active").eq(0).addClass("floor-active");
})
$(".floorcenter-button>li",".floor-mr").eq(1).click(function(){
	$(".floorcenter-main-imgbox",".floor-mr").animate({left:"-390px"});
	$(".floorcenter-button>li",".floor-mr").removeClass("floor-active").eq(1).addClass("floor-active");
})


// 运动栏目中间
$(".floor-centerbox>div",".floor-yd").hover(function(){
	$(".floorcenter-prev",".floor-yd").animate({left:"0"});
	$(".floorcenter-next",".floor-yd").animate({right:"0"});
},function(){
	$(".floorcenter-prev",".floor-yd").animate({left:"-30px"});
	$(".floorcenter-next",".floor-yd").animate({right:"-30px"});
})
$(".floorcenter-prev",".floor-yd").click(function(){
	$(".floorcenter-main-imgbox",".floor-yd").animate({left:"0"});
	$(".floorcenter-button>li",".floor-yd").removeClass("floor-active").eq(0).addClass("floor-active");
})
$(".floorcenter-next",".floor-yd").click(function(){
	$(".floorcenter-main-imgbox",".floor-yd").animate({left:"-390px"});
	$(".floorcenter-button>li",".floor-yd").removeClass("floor-active").eq(1).addClass("floor-active");
})
$(".floorcenter-button>li",".floor-yd").eq(0).click(function(){
	$(".floorcenter-main-imgbox",".floor-yd").animate({left:"0"});
	$(".floorcenter-button>li",".floor-yd").removeClass("floor-active").eq(0).addClass("floor-active");
})
$(".floorcenter-button>li",".floor-yd").eq(1).click(function(){
	$(".floorcenter-main-imgbox",".floor-yd").animate({left:"-390px"});
	$(".floorcenter-button>li",".floor-yd").removeClass("floor-active").eq(1).addClass("floor-active");
})


// 家居栏目中间
$(".floor-centerbox>div",".floor-jj").hover(function(){
	$(".floorcenter-prev",".floor-jj").animate({left:"0"});
	$(".floorcenter-next",".floor-jj").animate({right:"0"});
},function(){
	$(".floorcenter-prev",".floor-jj").animate({left:"-30px"});
	$(".floorcenter-next",".floor-jj").animate({right:"-30px"});
})
$(".floorcenter-prev",".floor-jj").click(function(){
	$(".floorcenter-main-imgbox",".floor-jj").animate({left:"0"});
	$(".floorcenter-button>li",".floor-jj").removeClass("floor-active").eq(0).addClass("floor-active");
})
$(".floorcenter-next",".floor-jj").click(function(){
	$(".floorcenter-main-imgbox",".floor-jj").animate({left:"-390px"});
	$(".floorcenter-button>li",".floor-jj").removeClass("floor-active").eq(1).addClass("floor-active");
})
$(".floorcenter-button>li",".floor-jj").eq(0).click(function(){
	$(".floorcenter-main-imgbox",".floor-jj").animate({left:"0"});
	$(".floorcenter-button>li",".floor-jj").removeClass("floor-active").eq(0).addClass("floor-active");
})
$(".floorcenter-button>li",".floor-jj").eq(1).click(function(){
	$(".floorcenter-main-imgbox",".floor-jj").animate({left:"-390px"});
	$(".floorcenter-button>li",".floor-jj").removeClass("floor-active").eq(1).addClass("floor-active");
})