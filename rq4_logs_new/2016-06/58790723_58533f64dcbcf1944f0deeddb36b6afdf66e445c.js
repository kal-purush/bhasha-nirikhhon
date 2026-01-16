window.onload = function () {
    var oBox=document.getElementById('box');
    var oBan=document.getElementById('ban');
    var oBtn=document.getElementById('btn');
    var aBtn=oBtn.getElementsByTagName('span');
    var aDiv=oBox.getElementsByTagName('li');

    for(var i=0; i<aBtn.length; i++){
        aBtn[i].index=i;
        aBtn[i].onclick=function(){
            tab(this.index);
            n=this.index;
        };
    }
    var n=0;
    function prev(){
        n--;
        if(n<0)n=aBtn.length-1;
        tab(n);
    }
    function next(){
        n++;
        if(n>aBtn.length-1)n=0;
        tab(n);
    }
    var timer=null;
    timer=setInterval(next, 1000);
    function tab(n){
        for(var i=0; i<aBtn.length; i++){
            aBtn[i].className='';
            aDiv[i].className='';
        }
        aBtn[n].className='active';
        aDiv[n].className='show';
    }
    oBox.onmouseover=function(){
        clearInterval(timer);
    };
    oBox.onmouseout=function(){
        clearInterval(timer);
        timer=setInterval(next, 1000);
    };
};