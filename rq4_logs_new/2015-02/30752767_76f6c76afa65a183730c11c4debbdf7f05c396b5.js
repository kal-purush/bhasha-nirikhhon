//Nguyen Thanh Liem - Ringier - 22 - 10 - 2012

function Dshowdc2210(mProperty,lstImage){
	kX = this;
	this.mE;
	function setMain(){
		//this.mainIpad = document.getElementById("mainIpad");
		this.speedLayers = new Array();
		this.curPos = new Array();
		//
		this.id = mProperty[0];
		this.ele = document.getElementById("mshowdc2210");
		this.Left = mProperty[1];
		this.Top = mProperty[2];
		this.Width = mProperty[3];
		this.Height = mProperty[4];
		setProperty(this.ele,null,"mshowdc2210",this.Left,this.Top,this.Width,this.Height);
		
		var layer1  = document.createElement("div");
		setProperty(layer1,"layer","layer",0,0,this.Width,this.Height);
		this.ele.appendChild(layer1);
	}
	
	//set Property
	function setProperty(ele, setId, setClass,  setLeft, setTop, setWidth, setHeight){
		if(setId != null){ele.setAttribute("id",setId);	}
		
		if(setClass != null){ ele.className = setClass;}
		
		if(setLeft != null){ele.style.left = setLeft + "px";}
		
		if(setTop != null){	ele.style.top = setTop + "px";}
		
		if(setWidth != null){ele.style.width = setWidth + "px";}
		
		if(setHeight != null){ele.style.height = setHeight + "px";}
	}
	
	function setPropertyFull(ele, setId, setClass,  setTop, setRight,  setBottom, setLeft, setWidth, setHeight){
		if(setRight != null){ele.style.right = setRight + "px";}
		
		if(setBottom != null){ele.style.bottom = setBottom + "px";}
		
		setProperty(ele, setId, setClass,  setLeft, setTop, setWidth, setHeight);
		
	}
		
	function init(){
		kX.mE = new setMain();
		
	}
	
	init();
}