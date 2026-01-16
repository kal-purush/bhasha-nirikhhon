var outConsoleBkj32a82Params = {
	lineNumber : 0, // the line number in the console that is started from 
	index : {},
	paused : false,
	stopped : false,
	bufferSize : 1000,
	pauseBuffer : [],
	defaultCellWidth : "50%",
	zIndex : 2147484000
};
function bark(mixed1, mixed2, params) {
	if (typeof params != "undefined")
		outConsoleBkj32a82Params = $.extend(outConsoleBkj32a82Params, params);
	params = outConsoleBkj32a82Params;
	if (!dev || params.stopped)
		return;
	if (params.paused)
		return params.pauseBuffer.push([mixed1,mixed2]);
	var 
	intResizeMargins = 5,
	intDefaultRightWidthPercentage = 70,
	intDefaultHeightPixels = 200,
	strIdentity = "outConsoleBkj32a82",
	$self = $("#"+strIdentity);
	// insert the value
	if ($self.length) {
		if (typeof mixed2 == "undefined") {
			params.lineNumber++;
			// left
			var $row = $("<div class='" + strIdentity + "Row'></div>")
			.css({
				backgroundColor:'#f7f7f7',
				cursor:'pointer',
				borderBottom:'1px solid #999',
				padding:4,
				marginBottom:1,
				maxHeight:20,
				overflow:'hidden',
				whiteSpace:'nowrap',
				textOverflow:'ellipsis'
			})
			.text(params.lineNumber+": "+$.dump(mixed1));
			$(params.index['leftPanel']).prepend($row);
			var arrChildren = $(params.index['leftPanel']).children();
			if (arrChildren.length > params.bufferSize) {
				$(arrChildren[arrChildren.length-1]).remove();
			}
		} else {
			// right
			var 
			strKeyName = strIdentity + "Cell" + encodeURIComponent(mixed1).replace(/[^a-z0-9_]/gi, "_"),
			$cell = $("#" + strKeyName),
			$key,
			$value;
			if (!$cell.length) {
				// init cell
				$cell = $("<div class='" + strIdentity + "Cell' id='" + strKeyName + "'></div>")
				.css({
					display:'inline-block',
					width:params.defaultCellWidth,
					cursor:'pointer',
					padding:1,
					margin:0,
					boxSizing:'border-box',
					backgroundColor:'#666'
				});
				$(params.index['rightPanel']).prepend($cell);
				$key = $("<div></div>")
				.css({
					display:'inline-block',
					maxWidth:100,
					lineHeight:'30px',
					padding:'0 10px 0 5px',
					color:'#fff',
					boxSizing:"border-box",
					overflow:'hidden',
					whiteSpace:'nowrap',
					textOverflow:'ellipsis'
				}).html(mixed1);
				$cell.append($key);
				$value = $("<div></div>")
				.css({
					display:'inline-block',
					boxSizing:"border-box",
					lineHeight:'30px',
					padding:'0 4px',
					color:'#fff',
					backgroundColor:'#888',
					overflow:'hidden',
					whiteSpace:'nowrap',
					textOverflow:'ellipsis'
				});
				$cell.append($value);
				fnFixDims();
				var arrChildren = $(params.index['rightPanel']).children();
				if (arrChildren.length > params.bufferSize) {
					$(arrChildren[arrChildren.length-1]).remove();
				}
			} else {
				//$key = $cell.find("td:first");
				$value = $cell.find("div:last");
			}
			$value.text($.dump(mixed2));
		}
	}
	else
	{
		// Init Bark!
		var 
		objEvents = {
			centerResize : {
				mousedown : function () {
					var $this = $(this);
					$(window).bind("mousemove."+strIdentity, function (e) {
						var intPercent = Math.min(100-intResizeMargins, Math.max(intResizeMargins, e.clientX / $(window).width() * 100));
						setCenter(intPercent);
						document.cookie = strIdentity+"CenterPosition=" + intPercent + ";";
						fnFixDims();
					});
					$(window).bind("mouseup."+strIdentity, function () {
						$(window).unbind("mouseup."+strIdentity);
						$(window).unbind("mousemove."+strIdentity);
					});
				}
			},
			topResize : {
				mousedown : function () {
					var $this = $(this);
					$(window).bind("mousemove."+strIdentity, function (e) {
						var intPercent = Math.min($(window).height()-intResizeMargins, Math.max(intResizeMargins, $(window).height()-e.clientY));
						setTop(intPercent);
						document.cookie = strIdentity+"TopPosition=" + intPercent + ";";
					});
					$(window).bind("mouseup."+strIdentity, function () {
						$(window).unbind("mouseup."+strIdentity);
						$(window).unbind("mousemove."+strIdentity);
					});
				}
			},
			leftPanel : {
				click : function (e) {
					$target = $(e.target);
					if ($target.hasClass("outConsoleBkj32a82Row")) {
						function close($this) {
							$this.remove();
							$(document).unbind("keyup.close"+strIdentity);
						}
						var strText = $target.text().replace(/^[0-9]+: /, ""),
						$content = $("<div style='z-index:" + (params.zIndex+2) + ";position:fixed;background-color:rgba(0,0,0,0.5);width:100%;height:100%;top:0;left:0;padding:60px;box-sizing:border-box;'></div>");
						$content.click(function (e) {
							if ($(e.target).is("textarea"))
								return;
							close($content);
						});
						$body = $("<textarea style='background-color:#fff;width:100%;height:100%;padding:10px;box-sizing:border-box;'></textarea>");
						$body.val(strText);
						$content.append($body); 
						$("#outConsoleBkj32a82").append($content);
						$body.focus()[0].setSelectionRange(0,0);
						$(document).bind("keyup.close"+strIdentity, function(e){
						    if(e.keyCode === 27)
						        close($content);
						});
					}
				}
			},
			rightPanel : {
				click : function (e) {
					var
					$target = $(e.target),
					$cell = $target.parent(".outConsoleBkj32a82Cell");
					if ($cell.length) {
						function close($this) {
							$this.remove();
							$(document).unbind("keyup.close"+strIdentity);
						}
						var strText = $target.text().replace(/^[0-9]+: /, "");
						var
						$content = $("<div style='z-index:" + (params.zIndex+2) + ";position:fixed;background-color:rgba(0,0,0,0.5);width:100%;height:100%;top:0;left:0;padding:60px;box-sizing:border-box;'></div>");
						$content.click(function (e) {
							if ($(e.target).is("textarea"))
								return;
							close($content);
						});
						$body = $("<textarea style='background-color:#fff;width:100%;height:100%;padding:10px;box-sizing:border-box;'></textarea>");
						$body.val(strText);
						$content.append($body); 
						$("#outConsoleBkj32a82").append($content);
						$body.focus()[0].setSelectionRange(0,0);
						$(document).bind("keyup.close"+strIdentity, function(e){
						    if(e.keyCode === 27)
						        close($content);
						});
					}
				}
			},
			minimize : {
				click : function () {
					var intTop = intResizeMargins+55;
					if (getCookie(strIdentity+"TopPosition") == intTop)
						intTop = intResizeMargins;
					setTop(intTop);
					document.cookie = strIdentity+"TopPosition=" + intTop + ";";
				}
			},
			maximize : {
				click : function () {
					var intTop = $(window).height()-intResizeMargins;
					if (getCookie(strIdentity+"TopPosition") == intTop)
						intTop = $(window).height();
					setTop(intTop);
					document.cookie = strIdentity+"TopPosition=" + intTop + ";";
				}
			},
			normalize : {
				click : function () {
					var intTop = intDefaultHeightPixels;
					if (getCookie(strIdentity+"TopPosition") == intTop)
						intTop = intTop+110;
					setTop(intTop);
					document.cookie = strIdentity+"TopPosition=" + intTop + ";";
				}
			},
			play : {
				click : function () {
					params.stopped = false;
					params.paused = false;
					while (params.pauseBuffer.length) {
						var arrItem = params.pauseBuffer.shift();
						bark(arrItem[0], arrItem[1]);
					}
				}
			},
			pause : {
				click : function () {
					params.stopped = false;
					params.paused = true;
				}
			},
			stop : {
				click : function () {
					params.stopped = true;
					params.paused = false;
				}
			},
			input : {
				click : function () {
					function close($this) {
						$this.remove();
						$(document).unbind("keyup.close"+strIdentity);
					}
					$content = $("<form><div style='z-index:" + (params.zIndex+2) + ";position:fixed;background-color:rgba(0,0,0,0.5);width:100%;height:100%;top:0;left:0;padding:60px;box-sizing:border-box;'></div></form>");
					$inner = $content.find("div:first");
					$inner.click(function (e) {
						if ($(e.target).is("textarea,input"))
							return;
						close($content);
					});
					$body = $("<textarea placeholder='Enter Javascript' style='background-color:#fff;width:100%;height:100%;padding:10px;box-sizing:border-box;'></textarea>");
					$save = $("<input class='" + strIdentity + "FocusBorder' type='submit' style='position:absolute;width:60px;right:70px;bottom:70px;background-color:#4cb0ff;color:#fff;border:0;' value='Run'>");
					$inner.append($body);
					$inner.append($save);
					$content.bind("submit", function (e) {
						e.preventDefault();
						var strCode = $body.val();
						if (strCode)
							eval(strCode);
					});
					$("#outConsoleBkj32a82").append($content);
					$body.focus();
					$(document).bind("keyup.close"+strIdentity, function(e){
					    if(e.keyCode === 27)
					        close($content);
					});
				}
			},
			reset : {
				click : function () {
					params.lineNumber = 0;
					$(params.index["leftPanel"]).find(">*").remove();
					$(params.index["rightPanel"]).find(">*").remove();
				}
			},
			close : {
				click : function () {
					params.stopped = true;
					params.paused = false;
					$self.remove();
				}
			}
		},
		getCookie = function (key) {
	  		var value = "; " + document.cookie;
	  		var parts = value.split("; " + key + "=");
	  		if (parts.length == 2) 
	  			return parts.pop().split(";").shift();
		},
		setCenter = function (intPercent) {
			$(params.index["centerResize"]).css({
				left : intPercent + "%"
			});
			$(params.index["leftPanel"]).css({
				width : intPercent + "%"
			});
			$(params.index["rightPanel"]).css({
				width : (100-intPercent) + "%",
				left : intPercent + "%"
			});
		},
		setTop = function (intPixels, boolFinal) {
			$self.css({
				height : intPixels
			});
			if (!boolFinal)
				fnFixDims(true);
		};
		objSkin = {
			title : {
				width:"100%",
				textAlign:'center',
				boxSizing:"border-box"
			},
			titleLink : {
				height:30,
				position:'absolute',
				fontFamily:'Tahoma, Arial Black, Arial Bold, Gadget, sans-serif',
				lineHeight:'30px',
				display:'inline-block',
				textShadow:'2px 2px 0 #000',
				color:"#fff",
				fontWeight:'bold',
				fontSize:'15px',
				opacity:0.5,
				marginTop:-1
			},
			barkHelper1 : {
				display:'inline-block',
				width:11,
				marginTop:-3,
				marginLeft:1,
				height:11,
				verticalAlign:"middle",
				lineHeight:0,
				perspective: '1000px',
				transform: 'rotateY(30deg)'
			},
			barkHelper2 : {
				display:'inline-block',
				height:"14%",
				boxShadow:"2px 2px 0 #000",
				transform: 'rotate(-28deg)',
				width:'70%',
				backgroundColor:"#fff",
				verticalAlign:'top',
				marginBottom:3
			},
			barkHelper3 : {
				display:'inline-block',
				height:"14%",
				boxShadow:"2px 2px 0 #000",
				transform: 'rotate(0deg)',
				width:'70%',
				backgroundColor:"#fff",
				verticalAlign:'top',
				marginBottom:3
			},
			barkHelper4 : {
				display:'inline-block',
				height:"14%",
				boxShadow:"2px 2px 0 #000",
				transform: 'rotate(28deg)',
				width:'70%',
				backgroundColor:"#fff",
				verticalAlign:'top'
			},
			topResize : {
				position:'absolute',
				height:6,
				width:'100%',
				backgroundColor:'#4cb0ff',
				cursor:'row-resize',
				zIndex:1,
				border:'0 solid #fff',
				borderWidth:'1px 0',
				top:-7
			},
			centerResize : {
				position:'absolute',
				width:6,
				marginLeft:-2,
				left:intDefaultRightWidthPercentage+'%',
				height:'100%',
				backgroundColor:'#4cb0ff',
				cursor:'col-resize',
				zIndex:1,
				border:'0 solid #fff',
				borderWidth:'0 2px'
			},
			topControls : {
				position:'absolute',
				height:30,
				width:'100%',
				backgroundColor:'#555',
				borderBottom:'2px solid #fff',
				zIndex:2
			},
			mainBody : {
				position:'relative',
				width:'100%',
				height:'100%',
				paddingTop:30,
				boxSizing:'border-box'
			},
			leftPanel : {
				float:'left',
				width:intDefaultRightWidthPercentage+'%',
				height:'100%',
				backgroundColor:'#eee',
				overflowY:'auto'
			},
			rightPanel : {
				float:'right',
				right:0,
				width:(100-intDefaultRightWidthPercentage)+'%',
				height:'100%',
				backgroundColor:'#555',
				overflowY:'auto',
				padding:'6px 3px 6px 13px',
				boxSizing:'border-box',
				lineHeight:'13px'
			},
			stopHelper : {
				width:14,
				height:14,
				backgroundColor:'#fff'
			},
			resetHelper1 : {
				marginLeft:5.3,
				marginBottom:-1,
				borderRadius:'1px 1px 0 0',
				width: 4,
  				height: 2,
  				backgroundColor:'#fff',
  				transform: 'rotate(5deg)'
			},
			resetHelper2 : {
				marginLeft:1,
				borderRadius:'7px 7px 2px 2px',
				width: 12,
  				height: 2,
  				backgroundColor:'#fff',
  				marginBottom:1,
  				transform: 'rotate(5deg)'
			},
			resetHelper3 : {
				marginLeft:2.5,
				borderRadius:'0 0 1.5px 1.5px',
				width: 9,
  				height: 9,
  				backgroundColor:'#fff'
			},
			inputHelper : {
				margin:2,
				width:10,
				height:10,
				borderRadius:5,
				backgroundColor:'#fff'
			},
			playHelper : {
				width: 0,
				height: 0,
				border: '0 solid transparent',
				borderWidth: '7px 0',
				borderLeft: '14px solid #fff'
			},
			pauseHelper : {
				width:6,
				height:14,
				backgroundColor:'#fff',
				float:'left'
			},
			pauseHelper2 : {
				width:6,
				height:14,
				backgroundColor:'#fff',
				float:'right'
			},
			minimize : {
				position:'relative',
				float:'right'
			},
			minimizeHelper : {
				backgroundColor:'#fff',
				marginTop:10,
				height:4
			},
			maximize : {
				verticalAlign:'top',
				float:'right',
				lineHeight:0
			},
			maximizeHelper1 : {
				verticalAlign:'top',
				display:'inline-block',
				backgroundColor:'#fff',
				height:11,
				width:3
			},
			maximizeHelper2 : {
				verticalAlign:'top',
				display:'inline-block',
				backgroundColor:'#fff',
				width:8,
				height:3
			},
			maximizeHelper3 : {
				verticalAlign:'top',
				display:'inline-block',
				backgroundColor:'#fff',
				width:3,
				height:11
			},
			maximizeHelper4 : {
				verticalAlign:'top',
				display:'inline-block',
				backgroundColor:'#fff',
				width:14,
				height:3
			},
			normalize : {
				verticalAlign:'top',
				float:'right',
				lineHeight:0,
				paddingTop:13
			},
			normalizeHelper1 : {
				verticalAlign:'top',
				display:'inline-block',
				backgroundColor:'#fff',
				height:6,
				width:3
			},
			normalizeHelper3 : {
				verticalAlign:'top',
				display:'inline-block',
				backgroundColor:'#fff',
				width:3,
				height:6
			},
			close : {
				position:'relative',
				float:'right'
			},
			closeHelper1 : {
				width:4,
				height:14,
				backgroundColor:'#fff',
				transform: 'rotate(45deg)',
				position:'absolute',
				left:14
			},
			closeHelper2 : {
				width:4,
				height:14,
				backgroundColor:'#fff',
				transform: 'rotate(-45deg)',
				position:'absolute',
				left:14
			},
			leftLine : {
				float:'left',
				paddingTop:6,
				width:12,
				height:18,
				display:'inline-block'
			},
			lineHelper : {
				width:5,
				borderRight:'2px solid #777',
				height:'100%'
			}
		};
		$self = $(
			"<div id='" + strIdentity + "'>"
				+ "<div c='topResize'></div>"
				+ "<div c='centerResize'></div>"
				+ "<div c='topControls'>"
					+ "<div c='title'><a target='_blank' class='outConsoleBkj32a82titleLink' href='http://barkconsole.com' c='titleLink'>Bark<div c='barkHelper1'><div c='barkHelper2'></div><div c='barkHelper3'></div><div c='barkHelper4'></div></div></a></div>"
					+ "<style>.outConsoleBkj32a82Cell * {cursor:pointer;} .outConsoleBkj32a82titleLink:hover {opacity:1!important};</style>"
					+ "<style>.outConsoleBkj32a82FocusBorder {border:2px solid transparent !important;}.outConsoleBkj32a82FocusBorder:focus {border:2px solid #555 !important}</style>"
					+ "<style>.outConsoleBkj32a82Button {opacity:0.5;display:inline-block;float:left;cursor:pointer;height:30px;width:31px;padding:8px;box-sizing:border-box;} .outConsoleBkj32a82Button * {cursor:pointer;} .outConsoleBkj32a82Button:hover</style>"
					+ "<style>.outConsoleBkj32a82Button:hover {opacity:1; background-color:rgba(255,255,255,0.2);};</style>"
					+ "<div c='reset' class='outConsoleBkj32a82Button'><div c='resetHelper1' class='outConsoleBkj32a82Helper'></div><div c='resetHelper2' class='outConsoleBkj32a82Helper'></div><div c='resetHelper3' class='outConsoleBkj32a82Helper'></div></div>"
					+ "<div c='leftLine'><div c='lineHelper'></div></div>"
					+ "<div c='play' class='outConsoleBkj32a82Button'><div c='playHelper' class='outConsoleBkj32a82Helper'></div></div>"
					+ "<div c='pause' class='outConsoleBkj32a82Button'><div c='pauseHelper' class='outConsoleBkj32a82Helper'></div><div c='pauseHelper2' class='outConsoleBkj32a82Helper'></div></div>"
					+ "<div c='stop' class='outConsoleBkj32a82Button'><div c='stopHelper' class='outConsoleBkj32a82Helper'></div></div>"
					+ "<div c='leftLine'><div c='lineHelper'></div></div>"
					+ "<div c='input' class='outConsoleBkj32a82Button'><div c='inputHelper' class='outConsoleBkj32a82Helper'></div></div>"
					+ "<div c='close' class='outConsoleBkj32a82Button'><div class='outConsoleBkj32a82Helper'><div c='closeHelper1'></div><div c='closeHelper2'></div></div></div>"
					+ "<div c='maximize' class='outConsoleBkj32a82Button'><div c='maximizeHelper1' class='outConsoleBkj32a82Helper'></div><div c='maximizeHelper2' class='outConsoleBkj32a82Helper'></div><div c='maximizeHelper3' class='outConsoleBkj32a82Helper'></div><div c='maximizeHelper4' class='outConsoleBkj32a82Helper'></div></div>"
					+ "<div c='normalize' class='outConsoleBkj32a82Button'><div c='normalizeHelper1' class='outConsoleBkj32a82Helper'></div><div c='maximizeHelper2' class='outConsoleBkj32a82Helper'></div><div c='normalizeHelper3' class='outConsoleBkj32a82Helper'></div><div c='maximizeHelper4' class='outConsoleBkj32a82Helper'></div></div>"
					+ "<div c='minimize' class='outConsoleBkj32a82Button'><div c='minimizeHelper' class='outConsoleBkj32a82Helper'></div></div>"
				+ "</div>"
				+ "<div c='mainBody'>"
					+ "<div c='leftPanel'></div>"
					+ "<div c='rightPanel'></div>"
				+ "</div>"
			+ "</div>"
		)
		.css({
			position:'fixed',
			bottom:0,
			left:0,
			width:'100%',
			height:intDefaultHeightPixels,
			zIndex:params.zIndex,
			userSelect: 'none',
			lineHeight:'23px',
			fontSize:'16px',
			font:'Helvetica,Verdana,sans-serif',
			color:'#000',
			letterSpacing:'0.2px',
			textAlign:'left',
			overflow:'visible',
			margin:0,
			padding:0,
			cursor:'default'
		});
		$self.find("*[c]").each(function () {
			var 
			$this = $(this),
			strName = $this.attr("c");
			params.index[strName] = this;
			if (objSkin[strName])
				$this.css(objSkin[strName]);
			$this.attr("id", strIdentity+'__'+strName);
			$this.removeAttr("c");
			var objEventItems = objEvents[strName];
			for (var strEvent in objEventItems) {
				$this.bind(strEvent, objEventItems[strEvent]);
			}
		});
		var intTop = getCookie(strIdentity+"TopPosition"),
		intLeft = getCookie(strIdentity+"CenterPosition");
		if (intTop)
			setTop(intTop);
		if (intLeft)
			setCenter(intLeft);
		$("body").prepend($self);
		$(window).bind("resize", fnFixDims);
		return bark(mixed1, mixed2)
	}
	
	function fnFixDims () {
		var 
		$rightPanel = $(params.index['rightPanel']),
		$cells = $rightPanel.find(".outConsoleBkj32a82Cell");
		if ($rightPanel.width() <400)
			$cells.css({
				width:"100%"
			});
		else if ($rightPanel.width() <800)
			$cells.css({
				width:"50%"
			});
		else if ($rightPanel.width() <1100)
			$cells.css({
				width:"33%"
			});
		else 
			$cells.css({
				width:"25%"
			});
		$cells.each(function () {
			var 
			$cell = $(this),
			$key = $cell.find("div:first"),
			$value = $cell.find("div:last");
			$value.css({
				width : ($cell.width()-$key.width()-17)
			});
		});
		if ($self.height()>$(window).height()) {
			setTop($(window).height()-intResizeMargins, true);
		}
	}
}