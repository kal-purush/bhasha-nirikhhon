window.onload = function() {
    var inner = document.getElementById("inner");
    var imgArr = document.getElementsByTagName("li");
    var spanArr = document.getElementsByTagName("span");
    var leftGo = document.getElementById("leftGo");
    var rightGo = document.getElementById("rightGo");
    var picWidth = 950; //获取第一张图片的宽度
    var index = 0;
    var timer = null;
    var AutoTimer = null;
    timer = setInterval(AutoGo,2000); //设置计时器
    leftGo.onclick=function(){
    Goleft(); //点击左边的小箭头
    };
    rightGo.onclick = function(){
    Goright(); //点击右边的小箭头
    };
    inner.onmouseover=function(){
    clearInterval(timer); //鼠标移入，清除计时器
    }
    inner.onmouseleave=function(){
    timer = setInterval(AutoGo,2000); //鼠标移出，启动计时器
    }
    function AutoGo(){
        // 自动轮播
        var start = inner.offsetLeft; //距离左边的边框的长度
        var end = - index * picWidth;  //终点
        var moveDistance = end - start;
        var speed = 20; //要走的步数
        var speedCount = 0;
        clearInterval(AutoTimer);
            // 清除之前的计时器，否则会越走越快
        clearInterval(timer);
        AutoTimer = setInterval(function(){
            speedCount++;
            if(speedCount >= speed) {
                // 步数足够
                clearInterval(AutoTimer);
                clearInterval(timer);
                timer = setInterval(Goright,1000);
                // 再次启动计时器
            }
            inner.style.left = moveDistance * speedCount/speed + start + "px";
        // 每步要走的距离
         },100)
        for(var i = 0; i < spanArr.length; i++){
            // 下标的样式改变，以及点击事件的绑定
            spanArr[i].index = i;
            // spanArr[i].className = "";
            // spanArr[index].className = "active";
            spanArr[i].onclick = function(){
                index = this.index; //传递当前点击的位置
                AutoGo();             
            }
        }
    }

    function Goleft(){
        // 往左走一步;
        
        if(index > 0){
            index--;
        }else{
            index = 5;
        }
        AutoGo();
         
    }
     
    function Goright() {
         // 往右走一步
        
        if(index < 5){
            index++;
        }else {
            index = 0;
        }
        AutoGo();
    }
}