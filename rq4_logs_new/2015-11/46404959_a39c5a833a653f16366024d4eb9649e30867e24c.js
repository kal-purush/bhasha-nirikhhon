/**
 * Created by taguchimunetaka on 2015/11/17.
 */

var SYUKOU = SYUKOU || {};

SYUKOU.COMMON = {};

SYUKOU.COMMON.FIXSIDEBAR = {

	SCROLL_NUM : 600,
	ANIMATE_SEC : 1000,

	init : function(){
		this.setParameters();
		this.prepare();
		this.bindEvent();
	},
	setParameters : function(){
		this.$window = $(window);
		this.htmlbody = $('html,body');
		this.$sidebox = $('.jsc-sidebox');
	},
	prepare : function(){
		this.boxOffsetTop = this.$sidebox.offset().top
	},
	bindEvent : function(){
		this.$window.on('scroll', $.proxy(this.fixsidebox, this));
	},
	fixsidebox : function(){
		var windowTop = this.$window.scrollTop()

		if (windowTop > this.boxOffsetTop) {
			this.$sidebox.css({
				'position': 'fixed',
				'top': 0
			});
		}else{
			this.$sidebox.css({
				'position': 'relative',
				'top':''
			});
		}
	}
};


$(function(){
	SYUKOU.COMMON.FIXSIDEBAR.init();
});