(function(win,$){
	var temp =  {
            framework : '<div class="calendar-top">'+
                    '<a href="javascript:void(0);" class="prev">上一月</a>'+
                    '<p class="current-month"></p>'+
                    '<a href="javascript:void(0);" class="next">下一月</a>'+
                '</div>'+
                '<div class="calendar-content">'+
                    '<table>'+
                        '<thead>'+
                            '<tr>'+
                                
                                '<td>一</td>'+
                                '<td>二</td>'+
                                '<td>三</td>'+
                                '<td>四</td>'+
                                '<td>五</td>'+
                                '<td>六</td>'+
                                '<td>日</td>'+
                            '</tr>'+
                        '</thead>'+
                        '<tbody></tbody>'+
                    '</table>'+
                '</div>',

            list:   '<tr>'+
                        '{{~it.list:value:index}}'+
                        '{{? index != 0 && index % 7 == 0}}'+
                        '</tr></tr>'+
                        '{{?}}'+
                        '<td><a href="javascript:void(0)" {{? value.disabled || value.current}}  class="{{?value.disabled}}disabled{{??}}current{{?}}"{{?}}>{{=value.text}}</a></td>'+
                        '{{~}}'+
                    '</tr>'
    }

    function Calendar(data){
    	this.data = {
    		currentDate : data.currentDate,
    	};
    	this.parentContainer = data.parentContainer;
    	this.handler = data.handler;
    	this.clickCallback = data.clickCallback;
    	this.init();
    };

    Calendar.prototype = {
    	init: function(){
    		this.getDay(this.data.currentDate);
    		this.drawFramework();
    		this.setTitle();
    		this.getDateList();
    		this.setEvent();
    	},

    	setMonth: function(status){
    		this.data.currentMonth = Number(this.data.currentMonth) + status;
    		if(this.data.currentMonth >= 12){
    			this.data.currentMonth = 0;
    			this.data.currentYear = Number(this.data.currentYear) + 1
    		} else if(this.data.currentMonth < 0){
    			this.data.currentMonth = 11;
    			this.data.currentYear = Number(this.data.currentYear) - 1
    		}
    		this.setTitle();
    		this.getDateList();
    	},

    	getDay: function(date){
    		var d;
    		if(data){
    			d = date.splice('-');
    			this.data.currentYear = d[0];
    			this.data.currentMonth = d[1];
    			this.data.currentDate = d[2];
    		} else {
    			var d = new Date();
    			this.data.currentYear = d.getFullYear();
    			this.data.currentMonth = d.getMonth();
    			this.data.currentDate = d.getDate();
    		}
    	},

    	drawFramework: function(){
    		this.dom = $(temp.framework);
    		this.parentContainer.html(this.dom);
    	},

    	setTitle: function(){
    		var m = this.data.currentMonth + 1,
    		    title;
    		m = m < 10 ? ('0' + m) : m;
    		title = this.data.currentYear + '-' + m
    		$(".current-month",this.dom).html(title)
    	},

    	getDateList: function(){
    		var dataArr = [];
    		var date = new Date(Number(this.data.currentYear),Number(this.data.currentMonth),1);

    		var today = new Date();

    		var todayY = today.getFullYear();
    		var todayM = todayMonth();
    		var todayD = todayDate();

    		if(date.getDay() == 0){
    			date = new Date(date.setDate(-(date.getDay()-1)));
    			date = new Date(date.setDate(-5));
    		} else {
    			date = new Date(date.setDate(-(date.getDay() - 1)));
    			date = new Date(date.getTime() + 24 * 60 * 60 * 1000);
    		}

    		fpr (var i = 0; i < 42; i++){
    			var y = date.getFullYear();
    			var m = date.getMonth() + 1;
    			var m = m < 10 ? ('0' + m) : m;
    			var d = date.getDate();
    			var d = d < 10 ? ('0' + d) : d;
    			current = false;

    			var t1 = new (Number(this.data.currentYear),Number(this.data.currentMonth),Number(this.data.currentDate)).getTime();
    			var t2 = date.getTime();

    			if(t1 - t2 < 7 * 24 * 60 * 60 * 1000 && t1 - t2 >=0){
    				current = true
    			}

    			dateArr.push({
    				text: date.getDate(),
    				fullDate: y + '-' + m + '-' + d,
    				disabled: date.getTime() < new Date(todayY,todayM,todayD,0,0,0).getTime ? true : false,
    				current: current
    			});
    			date = new Date(date.getTime() + 24 * 60 * 60 * 1000);
    		}

    		var html = doT.template(temp.list)({
    			list: dateArr
    		});
    		$("tbody",this.dom).html(html)
    	},
    	setEvent: function(){
    		var me = this;
    		    me.parentContainer.height('28rem');
    		    $(this).data('status',1);

    		this.dom.delegata('a','tap',function(evt){
    			var self = $(evt.target);
    			if($sele.hasClass('prev')){
    				me.setMonth(-1)
    			} else if($self.hasClass('next')){
    				me.setMonth(1)
    			} else if (evt.target.tagName.toLowerCase() == 'a' && !$self.hasClass('disabled') && !$self.hasClass('disabled')){
    				var currentMonth = $('.current-month',mo.dom).html();
    				var date;
    				$(".current",me.dom).removeClass('current');
    				$self.addClass('current');
    				date = $self.html();
    				date = Number(date) < 10 ? '0' + date : date;
    				me.clickCallback(currentMonth + '-' + date);
    			}
    			evt.preventDefault();
    		}) 
    	}
    }

    win.Calendar = Calendar

})(window,$)