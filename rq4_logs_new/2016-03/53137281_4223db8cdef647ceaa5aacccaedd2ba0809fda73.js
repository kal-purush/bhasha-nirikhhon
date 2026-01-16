// 本文件代码有点复杂费解，待重构

// 所有的图层
var layers = new Layers();

function drawBackground(canvas) {
	var context = canvas.getContext("2d");
	context.save();
	context.fillStyle = 'white';
	context.fillRect(0, 0, canvas.width, canvas.height);
	drawGrid(context, 'rgba(236, 236, 236, 1)', 20, 20);
	context.restore();
}
var iconSelectionIndex = 0;
var fillColorStyle = 'gray';
var selectedCurve = undefined; // 当前选中的贝塞尔曲线（段）

function initComponent() {
	// 图标工具条
	var iconToolBar = new IconToolBar("iconCanvas");

	iconToolBar.setIconSepecification(2, 32, 12);
	iconToolBar.setIcon(0, "images/pen.png");
	iconToolBar.setIcon(1, "images/fill.png");
	iconToolBar.setIcon(2, "images/path.png");
	iconToolBar.setIcon(3, "images/color.png");
	iconToolBar.setIcon(4, "images/zoomin.png");
	iconToolBar.setIcon(5, "images/zoomout.png");
	iconToolBar.setIcon(6, "images/canvasDrag.png");
	iconToolBar.setIcon(7, "images/move.png");
	
	var quickToolBars = [];
	// 常用工具，选择回掉函数
	iconToolBar.onSelectionChange = function(index) {
		iconSelectionIndex = index;
		if (3 == index) {
			colorPicker.show();
			pathView.hide();
		} else if (2 == index) {
			pathView.show();
			colorPicker.hide();
		} else {
			pathView.hide();
			colorPicker.hide();
		}
		showToolBar(index);

	}
	iconToolBar.drawIcons();
	
	function showToolBar(iconIndex){
		for(var i=0; i<quickToolBars.length; i++){
			if(iconIndex == quickToolBars[i].iconIndex){
				quickToolBars[i].toolBar.show();
			}
			else{
				quickToolBars[i].toolBar.hide();
			}
		}
	}
	// 颜色选择器
	var colorPicker = new PopupWindow();
	colorPicker.initializeEx = function() {
		this.redSlider = new COREHTML5.Slider('rgb(0,0,0)',
				'rgba(255,0,0,0.8)', 0),
			this.blueSlider = new COREHTML5.Slider('rgb(0,0,0)',
				'rgba(0,0,255,0.8)', 1.0),
			this.greenSlider = new COREHTML5.Slider('rgb(0,0,0)',
				'rgba(0,255,0,0.8)', 0.25),
			this.alphaSlider = new COREHTML5.Slider('rgb(0,0,0)',
				'rgba(255,255,255,0.8)', 0.5);
		this.redSlider.appendTo('redSliderDiv');
		this.blueSlider.appendTo('blueSliderDiv');
		this.greenSlider.appendTo('greenSliderDiv');
		this.alphaSlider.appendTo('alphaSliderDiv');
		this.alphaSlider.draw();
		this.redSlider.draw();
		this.greenSlider.draw();
		this.blueSlider.draw();
		var self = this;

		function updateColor() {
			var r = (self.redSlider.knobPercent * 255).toFixed(0),
				g = (self.greenSlider.knobPercent * 255).toFixed(0),
				b = (self.blueSlider.knobPercent * 255).toFixed(0),
				a = (self.alphaSlider.knobPercent * 255).toFixed(0);
			var colorStyle = 'rgba(' + r + ',' + g + ',' + b + ',' + a/255 + ')';
			iconToolBar.setIcon(3, "images/color.png", colorStyle);
			fillColorStyle = colorStyle;
		}

		// 拖动滑条，更新颜色
		this.redSlider.addChangeListener(function() {
			updateColor();
		});
		this.greenSlider.addChangeListener(function() {
			updateColor();
		});
		this.blueSlider.addChangeListener(function() {
			updateColor();
		});
		this.alphaSlider.addChangeListener(function() {
			updateColor();
		});
	}
	colorPicker.initialize("colorPicker", "mainBody", 300, 200);
	colorPicker.initializeEx();
	colorPicker.hide();

	var pathView = initLayerView();
	pathView.hide();
	
	quickToolBars = initQuickToolBars();
	showToolBar(iconSelectionIndex);
}

function initQuickToolBars(){
	var toolBars = [];
	var penQuickToolBar = new FoldableTooBar();
	penQuickToolBar.initialize("penToolQuickOperation", "mainBody", 600, 64, "top");
	penQuickToolBar.hide();
	penQuickToolBar.fold(); // 收起来
	toolBars.push({toolBar: penQuickToolBar, iconIndex: 0}); // 0对应于钢笔工具
	
	var selector = "#penToolQuickOperation #bezierRegular_00";
	var bg00 = document.querySelector(selector);
	bg00.onclick = function(e){
		if(undefined != selectedCurve){
			selectedCurve.applyRegular_00();
		}
	}
	
	selector = "#penToolQuickOperation #bezierRegular_0101";
	var bg0101 = document.querySelector(selector);
	bg0101.onclick = function(e){
		if(undefined != selectedCurve){
			selectedCurve.applyRegular_01(true);
		}
	}
	
	selector = "#penToolQuickOperation #bezierRegular_0102";
	var bg0102 = document.querySelector(selector);
	bg0102.onclick = function(e){
		if(undefined != selectedCurve){
			selectedCurve.applyRegular_01(false);
		}
	}
	return toolBars;
}

var bezierPathArray = new AdvancedArray();
bezierPathArray.push(new BezierPath());

layers.push(new Layer());
layers.get(0).push(bezierPathArray.get(0));
layers.setActiveLayer(0);

var bezierPathEdting = bezierPathArray.get(0);
var pathShow = true;
function initLayerView() {
	// 图层视图
	var pathView = new PopupWindow();
	pathView.initialize("pathView", "mainBody", 300, 400);
	pathView.setTitle("Path View");
	//pathView.hide();

	var pathList = document.querySelector("#pathView #pathList");
	pathList.oncontextmenu = function(e) {
		e.preventDefault();
	}
	var selectionIndex = 0;
	
	// 创建一条新路径
	// 20160102: 改造成添加新图层
	$('#pathView').on('click', '#add', function(e) {
		createNewPath();
		function createNewPath(){	
			var $items = $('.pathItem');
			
			var $pathItem = $('<div class="pathItem selected"></div>');
			var $imgSwitch = $('<img src="images/show.png" class="showSwitch">');
			$pathItem.append($imgSwitch);
			var $img = $('<img src="images/pathDemo.png" class="thumbnail">');
			$pathItem.append($img);
			var $inputTitle = $('<input type="text" class="title" value="new path">');
			$pathItem.append($inputTitle);
			$pathItem.append($('<p style="clear: both;"></p>'));
			var $refNode = $($items[0]);
			$pathItem.insertBefore($refNode);
			$items.removeClass('selected');
			// 创建一条新路径
			var pathNew = new BezierPath();
			pathNew.setName("new path");
			bezierPathArray.push(pathNew);
			selectionIndex = bezierPathArray.len-1;
			bezierPathEdting = bezierPathArray.get(selectionIndex);
			bezierPathEdting = pathNew;
			
			var layer  = new Layer("new layer");
			layer.push(pathNew);
			layers.push(layer);
			layers.print();
		}
	});
	
	$('#pathView').on('click', '.showSwitch', function(){	
		var parent = this.parentNode;	
		var $items = $('.pathItem');
		var index = undefined;
		for (var i = 0; i < $items.length; i++) {
			if ($items[i] == parent) {
				// 逆向
				index = $items.length-1-i;
				break;
			}
		}
		
		var path = bezierPathArray.get(index);
		// 隐藏
		if("1" == $(this).attr("show")){
			$(this).attr("show", "0");
			$(this).attr("src", "images/hide.png");
			path.showPath(false);
		}else{ // 显示
			$(this).attr("show", "1");
			$(this).attr("src", "images/show.png");
			path.showPath(true);
		}
	});
	
	$('#pathView').on('blur', '.title', function(e) {
		var path = bezierPathArray.get(selectionIndex);
		if(path){
			path.setName($(this).val());
		}	
	});
	
	$('#pathView').on('click', '#pathShowSwitch', function(e) {
		if(true == pathShow){
			pathShow = false;
			$(this).attr("src", "images/pathHide.png");
		}else{
			pathShow = true;
			$(this).attr("src", "images/pathShow.png");
		}
	});
	
	$('#pathView').on('click', '.pathItem', function(e) {
		var $items = $('.pathItem');
		$items.removeClass('selected');
		for (var i = 0; i < $items.length; i++) {
			if ($items[i] == this) {
				//alert("biggo: "+i);
				$($items[i]).addClass('selected');
				// 逆向
				selectionIndex = $items.length-1-i;
				bezierPathEdting = bezierPathArray.get(selectionIndex);
				break;
			}
		}
	});

	$('#pathView').on('click', '#up', function(e) {
		if (undefined != selectionIndex) {
			if (bezierPathArray.len-1 <= selectionIndex) {
				return;
			}
			var $items = $('.pathItem');
			//if(0 >= selectionIndex){
			//	return;
			//}
			// 在html路径视图里，索引大的路径显示在更前面
			var htmlElemIndex = bezierPathArray.len-1-selectionIndex;
			var $refNode = $($items[htmlElemIndex - 1]);
			var $tarNode = $($items[htmlElemIndex]);
			$tarNode.insertBefore($refNode);
			//$items.length-1-i;
			bezierPathArray.down(selectionIndex);
			selectionIndex += 1;
			bezierPathEdting = bezierPathArray.get(selectionIndex);
		}
	});
	
	$('#pathView').on('click', '#down', function(e) {
		if (undefined != selectionIndex) {
			if (0 >= selectionIndex) {
				return;
			}
			var $items = $('.pathItem');
			
			// 在html路径视图里，索引大的路径显示在更前面
			var htmlElemIndex = bezierPathArray.len-1-selectionIndex;
			var $refNode = $($items[htmlElemIndex + 1]);
			var $tarNode = $($items[htmlElemIndex]);
			$tarNode.insertAfter($refNode);
			//$items.length-1-i;
			bezierPathArray.up(selectionIndex);
			selectionIndex -= 1;
			bezierPathEdting = bezierPathArray.get(selectionIndex);
		}
	});
	return pathView;
}

function getSelectedIconIndex() {
	return iconSelectionIndex;
}

function getFillColorStyle() {
	return fillColorStyle;
}

var DefaultProcessor = function() {}
DefaultProcessor.prototype.onClick = function(e) {};
DefaultProcessor.prototype.onMouseDown = function(e) {

};
DefaultProcessor.prototype.onMouseMove = function(e) {

};
DefaultProcessor.prototype.onMouseUp = function(e) {
	dragParam = undefined;
};
DefaultProcessor.prototype.onDblClick = function(e) {};



function drawBezierPaths(context){
	for(var i=0; i<bezierPathArray.len; i++){
		var path = bezierPathArray.get(i);
		path.render(context, pathShow);
	}
	bezierPathEdting.showSelected(context);
}
// 跟缩放和Canvas平移相关的全局变量
var scale = 1;
var translateX = 0;
var translateY = 0;
// 标志刚刚拖拽过贝塞尔点，保证刚刚被选择的贝塞尔曲线，仍然处于被选择状态
var justDraggedBezierPoint = false; 

var selectedCurveIndex = -1;
var selectedBezierPoint = undefined;
var mouseRightButtonDown = false;
var interactionProcessor = [];
var dragParam = undefined;
function getScale(){
	return scale;
}
function windowToCanvas(canvas, x, y, scale, tranX, tranY) {
	var bbox = canvas.getBoundingClientRect();
	var canvasLoc = {
		x: ((x - bbox.left) * (canvas.width / bbox.width) - scale * tranX) / scale,
		y: ((y - bbox.top) * (canvas.height / bbox.height) - scale * tranY) / scale
	};
	return canvasLoc;
}

function getCanvasLocation(e) {
	var x = e.x || e.clientX;
	var y = e.y || e.clientY;
	var loc = windowToCanvas(canvas, x, y, scale, translateX, translateY);
	return loc;
}

// 钢笔工具的功能有增加，修改贝塞尔点
// 钢笔工具处理例程
var penProcessor = {
	onClick: function(e) {
		var loc = getCanvasLocation(e);
		if (false == justDraggedBezierPoint) {
			selectedCurve = undefined;
		} else {
			// 避免拖动了控制点之后，已被选择的曲线失去选择
			justDraggedBezierPoint = false;
			return;
		}
		var testResult = bezierPathEdting.curveSelectionTest(loc.x, loc.y);
		if (undefined != testResult) {
			selectedCurve = testResult.curve;
			selectedCurveIndex = testResult.index;
		}
	},
	onMouseDown: function(e) {
		var loc = getCanvasLocation(e);
		e.preventDefault();
		if (0 == e.button) {
			if (undefined != selectedCurve) {
				// 如果没有点中，则返回undefined
				selectedBezierPoint = selectedCurve.hitBezierPoint(loc.x, loc.y);
				canvas.style.cursor = 'pointer';
			} else {
				selectedBezierPoint = undefined;
			}
		} else if (2 == e.button) {
			mouseRightButtonDown = true;
		}
	},
	onMouseMove: function(e) {
		var loc = getCanvasLocation(e);
		if (undefined != selectedBezierPoint) {
			bezierPathEdting.updateBezierPoint({
				x: loc.x,
				y: loc.y,
				pointIndex: selectedBezierPoint.index,
				curveIndex: selectedCurveIndex
			});
			justDraggedBezierPoint = true;
		}
	},
	onMouseUp: function(e) {
		e.preventDefault();
		selectedBezierPoint = undefined;
		canvas.style.cursor = 'crosshair';
		if (mouseRightButtonDown) {
			mouseRightButtonDown = false;
			bezierPathEdting.closePath();
		}
	},
	onDblClick: function(e) {
		var loc = getCanvasLocation(e);
		bezierPathEdting.push({
			x: loc.x,
			y: loc.y
		});
	}
}

// 填充工具处理例程
var fillerProcessor = function() {}
fillerProcessor.prototype = new DefaultProcessor();
fillerProcessor.prototype.onClick = function(e) {
	bezierPathEdting.setFillStyle(getFillColorStyle());
}

var colorProcessor = function() {}
colorProcessor.prototype = new DefaultProcessor();
colorProcessor.prototype.onClick = function(e) {}

var ZoomInProcessor = function() {}
ZoomInProcessor.prototype = new DefaultProcessor();
ZoomInProcessor.prototype.onClick = function(e) {
	context.clearRect(0, 0, canvas.width, canvas.height);
	context.scale(1.25, 1.25);
	scale *= 1.25;
	translateX /= 1.25;
	translateY /= 1.25;
	context.clearRect(0, 0, canvas.width, canvas.height);
}
var ZoomOutProcessor = function() {} // 缩小
ZoomOutProcessor.prototype = new DefaultProcessor();
ZoomOutProcessor.prototype.onClick = function(e) {
	context.clearRect(0, 0, canvas.width, canvas.height);
	context.scale(0.8, 0.8);
	scale *= 0.8;
	context.clearRect(0, 0, canvas.width, canvas.height);
	translateX /= 0.8;
	translateY /= 0.8;
}


var CanvasDragProcessor = function() {}
CanvasDragProcessor.prototype = new DefaultProcessor();
CanvasDragProcessor.prototype.onMouseDown = function(e) {
	dragParam = {
		loc: getCanvasLocation(e)
	};
};

CanvasDragProcessor.prototype.onMouseUp = function(e) {
	loc = getCanvasLocation(e);
	var dx = loc.x - dragParam.loc.x;
	var dy = loc.y - dragParam.loc.y;

	//console.log("drag, dx: " + dx + ", dy: " + dy);
	context.clearRect(0, 0, canvas.width, canvas.height);
	translateX += dx / scale;
	translateY += dy / scale;
	context.translate((dx / scale), dy / scale);
	dragParam = undefined;
};

var PathMoveProcessor = function() {}
PathMoveProcessor.prototype = new DefaultProcessor();
PathMoveProcessor.prototype.onMouseDown = function(e) {
	dragParam = {
		loc: getCanvasLocation(e)
	};
};

PathMoveProcessor.prototype.onMouseMove = function(e) {
	if(undefined == dragParam){
		return;
	}
	loc = getCanvasLocation(e);
	
	var dx = loc.x - dragParam.loc.x;
	var dy = loc.y - dragParam.loc.y;
	dx = dx / scale;
	dy = dy / scale;
	//console.log("drag, dx: " + dx + ", dy: " + dy);
	if(bezierPathEdting){
		bezierPathEdting.move(dx, dy);
	}
	
	dragParam = {
		loc: getCanvasLocation(e)
	};
};

PathMoveProcessor.prototype.onMouseUp = function(e) {
	if(undefined == dragParam){
		return;
	}
	loc = getCanvasLocation(e);
	var dx = loc.x - dragParam.loc.x;
	var dy = loc.y - dragParam.loc.y;
	dx = dx / scale;
	dy = dy / scale;
	//console.log("drag, dx: " + dx + ", dy: " + dy);
	if(bezierPathEdting){
		bezierPathEdting.move(dx, dy);
	}
	
	dragParam = undefined;
};