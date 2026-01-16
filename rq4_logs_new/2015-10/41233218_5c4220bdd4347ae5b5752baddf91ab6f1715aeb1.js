
window.onload=function(){
		if(document.body.scrollTop){
			document.body.scrollTop=0;
		}else{
			document.documentElement.scrollTop=0;
		}
		pull();	
        window.onscroll=scrollLoadImg;
}
function scrollLoadImg(){
	var scrollTop=document.body.scrollTop||document.documentElement.scrollTop,
        	    winH=document.body.clientHeight||document.documentElement.clientHeight,
        	    main=document.getElementById('main'),
        	    boxs=getElementsByClass(main,'box'),
        	    lastBoxH=boxs[boxs.length-1].offsetHeight,
        	    lastBoxTopH=boxs[boxs.length-1].offsetTop,
        	    follow={"data":[{"src":"../images/27.jpg"},{"src":"../images/28.jpg"},{"src":"../images/29.jpg"},{"src":"../images/30.jpg"},{"src":"../images/31.jpg"}
        	    ,{"src":"../images/32.jpg"}
        	    ,{"src":"../images/33.jpg"}
        	    ,{"src":"../images/34.jpg"}]};
        	    if((lastBoxTopH+Math.floor(lastBoxH/2))<(scrollTop+winH)){
        	    	for(var i=0;i<follow.data.length;i++){
        	    		var newBox=document.createElement('div');
        	    		newBox.className='box';
        	    		var newPic=document.createElement('div');
        	    		newPic.className='pic';	
        	    		var img=document.createElement('img');
        	    		img.src=follow.data[i].src;
        	    		var desc=document.createElement('div');
        	    		desc.className='desc';
        	    		var imgTitle=document.createElement("h1");
        	    		imgTitle.className='img-title';
        	            imgTitle.appendChild(document.createTextNode('Hello'));
        	    		var imgDesc=document.createElement('p');
        	    		imgDesc.className='img-desc';
        	    		imgDesc.appendChild(document.createTextNode('this is loaded image'));
        	    		desc.appendChild(imgTitle);
        	    		desc.appendChild(imgDesc);
        	    		newPic.appendChild(img);
        	    		newPic.appendChild(desc);
        	    		newBox.appendChild(newPic);
        	    		main.appendChild(newBox);
        	    	}
        	    	pull();
        	    }
        	    
}
function pull(){
    var main=document.getElementById('main'),
	boxs=getElementsByClass(main,'box'),
	boxW=boxs[0].offsetWidth,
	winW=document.body.clientWidth||document.documentElement.clientWidth,
	cols=Math.floor(winW/boxW);
	main.style.cssText='width:'+cols*boxW+'px;margin:auto;',
	base=[],
	minH=0,
	minHIndex=0;
	for(var i=0;i<boxs.length;i++){
		     
            if(i<cols){
            	base.push(boxs[i].offsetHeight);
            }else{
            	boxs[i].style.position='absolute';
            	minH=Math.min.apply(null,base);
            	minHIndex=getMinIndex(base,minH);
            	boxs[i].style.top=minH+'px';
            	boxs[i].style.left=boxW*minHIndex+'px';
            	base[minHIndex]+=boxs[i].offsetHeight;
            }
	}
}
function getMinIndex(arr,minH){
	for(var i=0;i<arr.length;i++){
		if(arr[i]===minH){
			return i;
		}
	}
}
function getElementsByClass(parent,clsName){
	var childs=parent.getElementsByTagName('*');
	var oBox=[];
     for(var i=0;i<childs.length;i++){
          if(childs[i].className==clsName){
          	  oBox.push(childs[i]);
          }
     }
     return oBox;
}
function getImg(){

}