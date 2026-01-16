/**
 * jQuery Viewporter Plugin 1.0.0
 *
 * Copyright (c) 2015 Chris Mohrhauser
 *
 * Dual licensed under the MIT and GPLv2 licenses:
 *   http://www.opensource.org/licenses/mit-license.php
 *   http://www.gnu.org/licenses/gpl-2.0.html
 */



(function ( $ ) {
	$.viewporter = function(options){
		
		var _vp = this;
		
		_vp.curr = {w:0,h:0}; //current w/h
		
		_vp.settings = $.extend({},{ // default settings
		
			afixTo: 'body', //element(s) youll tie breakpoint classes to 
			addEvents: true, //recommended to always be true Addevents for onload and on resize
			writeOut: false, //if you want to write the info to an element on the page
			writeTo: '#viewporter_debug', // the element youd be writing to
			classPrefix: 'breakp-', //prefix for classes and breakpoints
			breakPoints: ['320', '480', '640', '720', '960', '1024'], //breakpoints to trigger and change classes
			
			//the follow is used if we are settings the viewport for you
			viewportIni: '1.0', //viewport initial scale
			viewportMax: '1.0', //viewport max scale
			viewportScale: '0' //viewport user scalable
			
		}, options);
		
		
		_vp.writeStuff = function(obj){
			$(_vp.settings.writeTo).html("Page size: width=" + obj.w + ", height=" + obj.h);	
		};
		
		
		_vp.getViewportSize = function(){ //distinctly chose to not use jquery here just for native speed
			var viewport = document.querySelector("meta[name=viewport]");
			if(viewport){
				var oldcontent = viewport.getAttribute('content');
				viewport.setAttribute('content', 'width=device-width, height=device-height, initial-scale=1.0, maximum-scale=1.0, user-scalable=0');
			}else{
				var meta = document.createElement('meta');
				meta.name = 'viewport';
				meta.content = 'width=device-width, height=device-height, initial-scale='+_vp.settings.viewportIni+', maximum-scale='+_vp.settings.viewportMax+', user-scalable='+_vp.settings.viewportScale;
				document.getElementsByTagName('head')[0].appendChild(meta);	
			}
			
			var w = window,d = w.document,ret = {};
		
			if (w.innerWidth != null){ // This works for all browsers except IE8 and before
				 ret = { w: w.innerWidth, h: w.innerHeight };
			}
			if (document.compatMode == "CSS1Compat" && !ret.w){ // For IE (or any browser) in Standards mode
				ret = { w: d.documentElement.clientWidth, h: d.documentElement.clientHeight };
			}
			if(!ret.w) ret = { w: d.body.clientWidth, h: d.body.clientHeight }; // For browsers in Quirks mode
			if(viewport && oldcontent){ //Reinit original viewport
				viewport.setAttribute('content', oldcontent);
			}
			_vp.curr = ret;
			_vp.deciferAssert();
			return ret;
		};
		
		
		_vp.addEvents = function(){ //distinctly native JS here for lack of obtrusion 
			if(window.attachEvent) {
				window.attachEvent('onresize', function(){ 
					v = _vp.getViewportSize(); 
					if(_vp.settings.writeOut){
						_vp.writeStuff(v);
					} 
				});
				document.attachEvent('onload', function(){ 
					v = _vp.getViewportSize(); 
					if(_vp.settings.writeOut){
						_vp.writeStuff(v);
					} 
				});
			}else if(window.addEventListener) {
				window.addEventListener('resize', function(){ 
					v = _vp.getViewportSize(); 
					if(_vp.settings.writeOut){
						_vp.writeStuff(v);
					} 
				}, true);
				document.addEventListener('load', function(){ 
					v = _vp.getViewportSize(); 
					if(_vp.settings.writeOut){
						_vp.writeStuff(v);
					} 
				}, true);
			}
		};
		
		
		_vp.deciferAssert = function(){
			var dret;
			for(var i in _vp.settings.breakPoints){
				dret = i;
				if(_vp.settings.breakPoints[i]>=_vp.curr.w){
					break;	
				}
			}
			if(_vp.settings.breakPoints[dret]<_vp.curr.w){
				$(_vp.settings.afixTo).attr('class', function(i, c){
					if(!c) return; return c.replace(new RegExp("(^|\\s)"+_vp.settings.classPrefix+"\\S+","g"), '');
				});
			}else{
				$(_vp.settings.afixTo).attr('class', function(i, c){
					if(!c) return; return c.replace(new RegExp("(^|\\s)"+_vp.settings.classPrefix+"\\S+","g"), '');
				}).addClass(_vp.settings.classPrefix+_vp.settings.breakPoints[dret]);
			}
		};
		
		
		return function(){
			if(_vp.settings.addEvents){ _vp.addEvents();	}
			return _vp.getViewportSize();
		}();
		
		
	}; 
}( jQuery ));	