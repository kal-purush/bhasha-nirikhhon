define(["Widget","jquery"],function(w,$){

	function Carousel(config){
		this.config={
			
		}
		this.extend(config);
		console.log(this.config);
	};
		
	Carousel.prototype=$.extend({},new w.Widget(),{
		renderUI:function(){
			this.container=$("<div class='carousel_container'></div>");
			this.prev_btn=$("<div class='carousel_btn prev_btn'></div>");
			this.next_btn=$("<div class='carousel_btn next_btn'></div>");
			this.pics=$("<div class='carousel_pics'></div>");
			this.pic_list=$("<ul class='carousel_list'></ul>");
			if(this.config.pics.length>0){
				this.pic_item=$("<li class='carousel_item'>");
			}
		},
		//为Carousel中的元素绑定事件
		bindUI:function(){
			if(this.config.buttons!=null){
				for (var i =0;i<= this.config.buttons.length - 1;i++) {
					//btn绑定事件
					var btn=this.config.buttons[i];
					if(btn.callBack!=null){
						this.container.delegate('.'+btn.type+'_btn',
							'click',btn.callBack);
					}
				}
			}
		},
		show:function(){
			this.fire('show');
		},
		hide:function(){
			this.fire('show');
		}
	});

return {Carousel:Carousel};
});