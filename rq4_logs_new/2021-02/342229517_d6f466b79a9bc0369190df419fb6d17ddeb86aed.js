$(function(){
  // random
  var pic_num = 1 + Math.floor( Math.random() * 9 );
  $("#wrapper").addClass("pic"+pic_num);

  // datepicker
  $("#dp").datepicker({
    dateFormat: "yy/mm/dd",
    showOn: "button",
    buttonImageOnly: true,
    onSelect:function(dateText){
      localStorage.hello = dateText;
      location.reload();
    }
  });
});

// countdown
function CountdownTimer(elm,tl,mes){
  this.initialize.apply(this,arguments);
}
CountdownTimer.prototype={
  initialize:function(elm,tl,mes){
    this.elem = elm;
    this.tl = tl;
    this.mes = mes;
  },countDown:function(){
    var timer='';
    var today=new Date();
    var day=Math.floor((this.tl-today)/(24*60*60*1000));
    if(day < 10){
      console.log(day);
      var day = "00"+day;
      console.log(day);
    }else if(day < 100){
      console.log(day);
      var day = "0"+day;
      console.log(day);
    }
    var hour=Math.floor(((this.tl-today)%(24*60*60*1000))/(60*60*1000));
    var min=Math.floor(((this.tl-today)%(24*60*60*1000))/(60*1000))%60;
    var sec=Math.floor(((this.tl-today)%(24*60*60*1000))/1000)%60%60;
    var me=this;

    if( ( this.tl - today ) > 0 ){
      timer += '<div class="day clearfix"><span class="num'+this.addZeroD(day).charAt(0)+'"></span><span class="num'+this.addZeroD(day).charAt(1)+'"></span><span class="num'+this.addZeroD(day).charAt(2)+'"></span></div><div class="hour clearfix"><span class="num'+this.addZero(hour).charAt(0)+'"></span><span class="num'+this.addZero(hour).charAt(1)+'"></span></div><div class="min clearfix"><span class="num'+this.addZero(min).charAt(0)+'"></span><span class="num'+this.addZero(min).charAt(1)+'"></span></div><div class="sec clearfix"><span class="num'+this.addZero(sec).charAt(0)+'"></span><span class="num'+this.addZero(sec).charAt(1)+'"></span></div>';
      $("."+this.elem).html(timer);
      tid = setTimeout( function(){me.countDown();},1000);
    }else{
      $("."+this.elem).html(this.mes);
      return;
    }
  },addZero:function(num){ return ('0'+num).slice(-2); },addZeroD:function(num){ return ('0'+num).slice(-3); }
}

function cdt(){
  var tl = new Date(localStorage.hello);
  var timer = new CountdownTimer('cdt',tl,'<div class="day clearfix"><span></span><span></span><span></span></div><div class="hour clearfix"><span></span><span></span></div><div class="min clearfix"><span></span><span></span></div><div class="sec clearfix"><span></span><span></span></div>');
  timer.countDown();
}
window.onload=function(){
  cdt();
}
