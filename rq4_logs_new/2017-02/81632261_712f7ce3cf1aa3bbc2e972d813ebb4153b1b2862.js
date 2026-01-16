		window.onload=function(){
		var cas=document.querySelector('canvas');
		alert(cas);
		var ctx=cas.getContext('2d');
		var currX=cas.width/2,
			currY=cas.height/2;
		// 一开始默认在正中间有一个矩形，鼠标点击哪里，矩形就移动到哪来
		ctx.fillRect(currX-10,currY-10,20,20);
		//添加鼠标的点击事件获得目标坐标
		var intervalId;
		cas.onclick=function(e){
		clearInterval(intervalId);
		var x=e.offsetX,
			y=e.offsetY;
		intervalId=setInterval(function(){
			// 重绘
			// 1.判断是否已经非常接近目标，如果接近则直接将currX和currY
			// 等于目标坐标，否则就按照增量的办法去currX+=deltaX和currY+=deltaY
			var distanceX=Math.abs(x-currX);
			var distanceY=Math.abs(y-currY);
			var distanceZ=parseInt(Math.pow(Math.pow((distanceX+distanceY),2),1/2));
			// console.log(distanceZ);
			// 比例：以z的增量为基准
			// deltaX/deltaZ==distanceX/distanceZ;
			var k1=distanceX/distanceZ;
			var k2=distanceY/distanceZ;
			// 设置增量
			var deltaZ=1;
			var deltaX=deltaZ*k1;
			var deltaY=deltaZ*k2;
			// 方向标记
			var markX=x>currX?1:-1;
			var markY=y>currY?1:-1;
			// 判断临界
			if(Math.abs(currX-x)<=deltaX&&Math.abs(currY-y)<=deltaY){
				currX=x;
				currY=y;
				clearInterval(intervalId);
			}else{
				currX+=markX*deltaX;
				currY+=markY*deltaY;
			}
			// 擦除
			ctx.clearRect(0,0,cas.width,cas.height);
			// 重绘
			ctx.fillRect(currX-10,currY-10,20,20);

		},16.67);
	}

	
		}
		