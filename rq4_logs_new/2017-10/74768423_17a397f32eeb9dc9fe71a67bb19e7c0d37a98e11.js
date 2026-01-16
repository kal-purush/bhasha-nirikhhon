/**
 * @fileoverview 百度地图的轨迹跟随类，对外开放。
 * 用户可以在地图上自定义轨迹运动
 * 可以自定义路过某个点的图片，文字介绍等。
 * 主入口类是<a href="symbols/BMapLib.LuShu.html">LuShu</a>，
 * 基于Baidu Map API 1.2。.
 *
 * @author Baidu Map Api Group
 * @version 1.2
 */

/**
 * @namespace BMap的所有library类均放在BMapLib命名空间下
 */
var BMapLib = window.BMapLib = BMapLib || {};

(function() {
    //声明baidu包
    var T, baidu = T = baidu || {version: '1.5.0'};
    baidu.guid = '$BAIDU$';
    //以下方法为百度Tangram框架中的方法，请到http://tangram.baidu.com 查看文档
    (function() {
        window[baidu.guid] = window[baidu.guid] || {};
        baidu.dom = baidu.dom || {};
        baidu.dom.g = function(id) {
            if ('string' == typeof id || id instanceof String) {
                return document.getElementById(id);
            } else if (id && id.nodeName && (id.nodeType == 1 || id.nodeType == 9)) {
                return id;
            }
            return null;
        };
        baidu.g = baidu.G = baidu.dom.g;
        baidu.lang = baidu.lang || {};
        baidu.lang.isString = function(source) {
            return '[object String]' == Object.prototype.toString.call(source);
        };
        baidu.isString = baidu.lang.isString;
        baidu.dom._g = function(id) {
            if (baidu.lang.isString(id)) {
                return document.getElementById(id);
            }
            return id;
        };
        baidu._g = baidu.dom._g;
        baidu.dom.getDocument = function(element) {
            element = baidu.dom.g(element);
            return element.nodeType == 9 ? element : element.ownerDocument || element.document;
        };
        baidu.browser = baidu.browser || {};
        baidu.browser.ie = baidu.ie = /msie (\d+\.\d+)/i.test(navigator.userAgent) ? (document.documentMode || + RegExp['\x241']) : undefined;
        baidu.dom.getComputedStyle = function(element, key) {
            element = baidu.dom._g(element);
            var doc = baidu.dom.getDocument(element),
                styles;
            if (doc.defaultView && doc.defaultView.getComputedStyle) {
                styles = doc.defaultView.getComputedStyle(element, null);
                if (styles) {
                    return styles[key] || styles.getPropertyValue(key);
                }
            }
            return '';
        };
        baidu.dom._styleFixer = baidu.dom._styleFixer || {};
        baidu.dom._styleFilter = baidu.dom._styleFilter || [];
        baidu.dom._styleFilter.filter = function(key, value, method) {
            for (var i = 0, filters = baidu.dom._styleFilter, filter; filter = filters[i]; i++) {
                if (filter = filter[method]) {
                    value = filter(key, value);
                }
            }
            return value;
        };
        baidu.string = baidu.string || {};


        baidu.string.toCamelCase = function(source) {

            if (source.indexOf('-') < 0 && source.indexOf('_') < 0) {
                return source;
            }
            return source.replace(/[-_][^-_]/g, function(match) {
                return match.charAt(1).toUpperCase();
            });
        };
        baidu.dom.getStyle = function(element, key) {
            var dom = baidu.dom;
            element = dom.g(element);
            key = baidu.string.toCamelCase(key);

            var value = element.style[key] ||
                        (element.currentStyle ? element.currentStyle[key] : '') ||
                        dom.getComputedStyle(element, key);

            if (!value) {
                var fixer = dom._styleFixer[key];
                if (fixer) {
                    value = fixer.get ? fixer.get(element) : baidu.dom.getStyle(element, fixer);
                }
            }

            if (fixer = dom._styleFilter) {
                value = fixer.filter(key, value, 'get');
            }
            return value;
        };
        baidu.getStyle = baidu.dom.getStyle;
        baidu.dom._NAME_ATTRS = (function() {
            var result = {
                'cellpadding': 'cellPadding',
                'cellspacing': 'cellSpacing',
                'colspan': 'colSpan',
                'rowspan': 'rowSpan',
                'valign': 'vAlign',
                'usemap': 'useMap',
                'frameborder': 'frameBorder'
            };

            if (baidu.browser.ie < 8) {
                result['for'] = 'htmlFor';
                result['class'] = 'className';
            } else {
                result['htmlFor'] = 'for';
                result['className'] = 'class';
            }

            return result;
        })();
        baidu.dom.setAttr = function(element, key, value) {
            element = baidu.dom.g(element);
            if ('style' == key) {
                element.style.cssText = value;
            } else {
                key = baidu.dom._NAME_ATTRS[key] || key;
                element.setAttribute(key, value);
            }
            return element;
        };
        baidu.setAttr = baidu.dom.setAttr;
        baidu.dom.setAttrs = function(element, attributes) {
            element = baidu.dom.g(element);
            for (var key in attributes) {
                baidu.dom.setAttr(element, key, attributes[key]);
            }
            return element;
        };
        baidu.setAttrs = baidu.dom.setAttrs;
        baidu.dom.create = function(tagName, opt_attributes) {
            var el = document.createElement(tagName),
                attributes = opt_attributes || {};
            return baidu.dom.setAttrs(el, attributes);
        };
        baidu.object = baidu.object || {};
        baidu.extend =
        baidu.object.extend = function(target, source) {
            for (var p in source) {
                if (source.hasOwnProperty(p)) {
                    target[p] = source[p];
                }
            }
            return target;
        };
    })();

    /**
     * @exports LuShu as BMapLib.LuShu
     */
    var LuShu =
    /**
     * LuShu类的构造函数
     * @class LuShu <b>入口</b>。
     * 实例化该类后，可调用,start,end,pause等方法控制覆盖物的运动。

     * @constructor
         * @param {Map} map Baidu map的实例对象.
         * @param {Array} path 构成路线的point的数组.
         * @param {Json Object} opts 可选的输入参数，非必填项。可输入选项包括：<br />
         * {<br />"<b>landmarkPois</b>" : {Array} 要在覆盖物移动过程中，显示的特殊点。格式如下:landmarkPois:[<br />
         *      {lng:116.314782,lat:39.913508,html:'加油站',pauseTime:2},<br />
         *      {lng:116.315391,lat:39.964429,html:'高速公路收费站,pauseTime:3}]<br />
         * <br />"<b>icon</b>" : {Icon} 覆盖物的icon,
         * <br />"<b>speed</b>" : {Number} 覆盖物移动速度，单位米/秒    <br />
         * <br />"<b>defaultContent</b>" : {String} 覆盖物中的内容    <br />
         * }<br />.
         * @example <b>参考示例：</b><br />
         * var lushu = new BMapLib.LuShu(map,arrPois,{defaultContent:"从北京到天津",landmarkPois:[]});
     */
     BMapLib.LuShu = function(map, pathList, opts) {
        if (!pathList || pathList.length < 1) {
            return;
        }
        this._map = map;
        
        // 轨迹当前段落的索引
        this._pathIndex = 0;
        this._pathList = pathList;
        //存储一条路线
        this._path = pathList[this._pathIndex].path;
        
        //移动到当前点的索引
        this.i = 0;
        //控制暂停后开始移动的队列的数组
        this._setTimeoutQuene = [];
        //进行坐标转换的类
        this._projection = this._map.getMapType().getProjection();
        this._opts = {
            icon: null,
            //默认速度 米/秒
            speed: 4000,
            defaultContent: ''
        };
        this._setOptions(opts);
        this.speed = pathList[this._pathIndex].speed;
        // 新增暂停时的位置 解决下一轮开始闪现到下一个点 @author denglt
        this.pausePos = null;
        //如果不是默认实例，则使用默认的icon
        if (!this._opts.icon instanceof BMap.Icon) {
            this._opts.icon = defaultIcon;
        }
    }
     /**
     * 根据用户输入的opts，修改默认参数_opts
     * @param {Json Object} opts 用户输入的修改参数.
     * @return 无返回值.
     */
    LuShu.prototype._setOptions = function(opts) {
        if (!opts) {
            return;
        }
        for (var p in opts) {
            if (opts.hasOwnProperty(p)) {
                this._opts[p] = opts[p];
            }
        }
    }

    /**
     * @description 开始运动
     * @param none
     * @return 无返回值.
     *
     * @example <b>参考示例：</b><br />
     * lushu.start();
     */
    LuShu.prototype.start = function() {
        var me = this,
            len = me._path.length;
        //不是第一次点击开始,并且小车还没到达终点
        if (me.i && me.i < len - 1) {
            //没按pause再按start不做处理
            if (!me._fromPause) {
                return;
            }else if(!me._fromStop){
	            //按了pause按钮,并且再按start，直接移动到下一点
	            //并且此过程中，没有按stop按钮
                //防止先stop，再pause，然后连续不停的start的异常
                
	            me._moveNext(++me.i);
            }
        }else {
            //第一次点击开始，或者点了stop之后点开始
            me._addMarker();
            //等待marker动画完毕再加载infowindow
            me._timeoutFlag = setTimeout(function() {
                    me._addInfoWin();
                    me._moveNext(me.i);
            },400);
        }
         //重置状态
        this._fromPause = false;
        this._fromStop = false;
    },
    /**
     * 结束运动
     * @return 无返回值.
     *
     * @example <b>参考示例：</b><br />
     * lushu.stop();
     */
    LuShu.prototype.stop = function() {
        this.i = 0;
        this._fromStop = true;
        clearInterval(this._intervalFlag);
        this._clearTimeout();
        //重置landmark里边的poi为未显示状态
        for (var i = 0, t = this._opts.landmarkPois, len = t.length; i < len; i++) {
            t[i].bShow = false;
        }
    };
    /**
     * 暂停运动
     * @return 无返回值.
     */
    LuShu.prototype.pause = function() {
        clearInterval(this._intervalFlag);
        
        //标识是否是按过pause按钮
        this._fromPause = true;
        this._clearTimeout();
    };
    /**
     * 隐藏上方overlay
     * @return 无返回值.
     *
     * @example <b>参考示例：</b><br />
     * lushu.hideInfoWindow();
     */
    LuShu.prototype.hideInfoWindow = function() {
        this._overlay._div.style.visibility = 'hidden';
    };
    /**
     * 显示上方overlay
     * @return 无返回值.
     *
     * @example <b>参考示例：</b><br />
     * lushu.showInfoWindow();
     */
    LuShu.prototype.showInfoWindow = function() {
        this._overlay._div.style.visibility = 'visible';
    };
    //Lushu私有方法
    baidu.object.extend(LuShu.prototype, {
        /**
         * 添加marker到地图上
         * @param {Function} 回调函数.
         * @return 无返回值.
         */
        _addMarker: function(callback) {
            if (this._marker) {
                this.stop();
                this._map.removeOverlay(this._marker);
                clearTimeout(this._timeoutFlag);
            }
            //移除之前的overlay
            this._overlay && this._map.removeOverlay(this._overlay);
            var marker = new BMap.Marker(this._path[0]);
            this._opts.icon && marker.setIcon(this._opts.icon);
            this._map.addOverlay(marker);
            // marker.setAnimation(BMAP_ANIMATION_DROP);
            this._marker = marker;
        },
        /**
         * 添加上方overlay
         * @return 无返回值.
         */
        _addInfoWin: function() {
            var me = this;
            var overlay = new CustomOverlay(me._marker.getPosition(), me._opts.defaultContent);

            //将当前类的引用传给overlay。
            overlay.setRelatedClass(this);
            this._overlay = overlay;
            this._map.addOverlay(overlay);
        },
        /**
         * 获取墨卡托坐标
         * @param {Point} poi 经纬度坐标.
         * @return 无返回值.
         */
        _getMercator: function(poi) {
            return this._map.getMapType().getProjection().lngLatToPoint(poi);
        },
        /**
         * 计算两点间的距离
         * @param {Point} poi 经纬度坐标A点.
         * @param {Point} poi 经纬度坐标B点.
         * @return 无返回值.
         */
        _getDistance: function(pxA, pxB) {
            return Math.sqrt(Math.pow(pxA.x - pxB.x, 2) + Math.pow(pxA.y - pxB.y, 2));
        },
          //目标点的  当前的步长,position,总的步长,动画效果,回调
        /**
         * 移动小车
         * @param {Number} poi 当前的步长.
         * @param {Point} initPos 经纬度坐标初始点.
         * @param {Point} targetPos 经纬度坐标目标点.
         * @param {Function} effect 缓动效果.
         * @return 无返回值.
         */
        _move: function(initPos,targetPos,effect) {
            var me = this,
                //当前的帧数
                currentCount = 0,
                //步长，米/秒
                timer = 10,
                step = this.speed / (1000 / timer),
                //初始坐标
                initPos = this._projection.lngLatToPoint(initPos),
                //获取结束点的(x,y)坐标
                targetPos = this._projection.lngLatToPoint(targetPos),
                //总的步长
                count = Math.round(me._getDistance(initPos, targetPos) / step);

            //如果小于1直接移动到下一点
            
            if (count < 1) {
                me._moveNext(++me.i);
                return;
            }
            //两点之间匀速移动
            me._intervalFlag = setInterval(function() {
            //两点之间当前帧数大于总帧数的时候，则说明已经完成移动
	            if (currentCount >= count) {
	                clearInterval(me._intervalFlag);
                    //移动的点已经超过总的长度
		        	if(me.i > me._path.length){
						return;
		        	}
		        	//运行下一个点
	                me._moveNext(++me.i);
	            }else {
	                    //正在移动
                        currentCount++;
                        var p = null;
                        if (me.i - 1 > 0) {
                            p = me._path[i.i - 1]
                        }
	                    var x = effect(initPos.x, targetPos.x, currentCount, count),
	                        y = effect(initPos.y, targetPos.y, currentCount, count),
                            pos = me._projection.pointToLngLat(new BMap.Pixel(x, y));
                        if(currentCount == 1){
                            if (me._opts.enableRotation == true) {
                                me.setRotation(p, initPos,targetPos)
                            }
                        }
                            //设置marker
	                    me._marker.setPosition(pos);
	                    //设置自定义overlay的位置
                        me._setInfoWin(pos);
                        me.pausePos = pos;
	                }
	        },timer);
        },
        setRotation: function(l, f, m) {
            var j = this;
            var e = 0;
            f = j._map.pointToPixel(f);
            m = j._map.pointToPixel(m);

            console.log('initPos', f)
            console.log('rotation', m)
            if (m.x != f.x) {
                var k = (m.y - f.y) / (m.x - f.x),
                g = Math.atan(k);
                e = g * 360 / (2 * Math.PI);
                if (m.x < f.x) {
                    e = -e + 90 + 90
                } else {
                    e = -e
                }
                console.log('rotation', - e)
                j._marker.setRotation( - e)
            } else {
                var h = m.y - f.y;
                var i = 0;
                if (h > 0) {
                    i = -1
                } else {
                    i = 1
                }
                j._marker.setRotation( - i * 90)
            }
            return
        },
        /**
         * 移动到下一个点
         * @param {Number} index 当前点的索引.
         * @return 无返回值.
         */
        _moveNext: function(index) {
            var me = this;
            if (index < this._path.length - 1) {
                me._move(me._path[index], me._path[index + 1], me._tween.linear);
            }else{
                console.log('changePath')
                if(me._pathIndex < me._pathList.length - 1){
                    me._pathIndex++;
                    me._resetPath(me._pathList[me._pathIndex])
                    if(me._path.length == 2){
                        me._move(me._path[0], me._path[1], me._tween.linear);
                    }else{
                        me._moveNext(++me.i);
                    }
                }
            }
        },

        /**
         * 暂停后把上一次的移动跑完
         * @author denglt
         */
        _againMove: function(){
            console.log('againmove')
            var me = this,
                len = me._path.length;
            if (me.i < len - 1) {
                //没按pause再按start 或者没有开始过不做处理
                if (!me._fromPause || me.pausePos == null) {
                    return;
                }
                me._fromPause = false;
                me._move(me.pausePos,me._path[++me.i], me._tween.linear)
            }
        },
        /**
         * 设置小车上方infowindow的内容，位置等
         * @param {Point} pos 经纬度坐标点.
         * @return 无返回值.
         */
        _setInfoWin: function(pos) {
            //设置上方overlay的position
            var me = this;
            me._overlay.setPosition(pos, me._marker.getIcon().size);
            var index = me._troughPointIndex(pos);
            if (index != -1) {
                clearInterval(me._intervalFlag);
                me._overlay.setHtml(me._opts.landmarkPois[index].html);
                me._overlay.setPosition(pos, me._marker.getIcon().size);
                me._pauseForView(index);
            }else {
                me._overlay.setHtml(me._opts.defaultContent);
            }
        },
        /**
         * 在某个点暂停的时间
         * @param {Number} index 点的索引.
         * @return 无返回值.
         */
        _pauseForView: function(index) {
            var me = this;
            var t = setTimeout(function() {
                //运行下一个点
                me._moveNext(++me.i);
            },me._opts.landmarkPois[index].pauseTime * 1000);
            me._setTimeoutQuene.push(t);
        },
         //清除暂停后再开始运行的timeout
        _clearTimeout: function() {
            for (var i in this._setTimeoutQuene) {
                clearTimeout(this._setTimeoutQuene[i]);
            }
            this._setTimeoutQuene.length = 0;
        },
         //缓动效果
        _tween: {
            //初始坐标，目标坐标，当前的步长，总的步长
            linear: function(initPos, targetPos, currentCount, count) {
                var b = initPos, c = targetPos - initPos, t = currentCount,
                d = count;
                return c * t / d + b;
            }
        },

        /**
         * 否经过某个点的index
         * @param {Point} markerPoi 当前小车的坐标点.
         * @return 无返回值.
         */
        _troughPointIndex: function(markerPoi) {
            var t = this._opts.landmarkPois, distance;
            for (var i = 0, len = t.length; i < len; i++) {
                //landmarkPois中的点没有出现过的话
                if (!t[i].bShow) {
                    distance = this._map.getDistance(new BMap.Point(t[i].lng, t[i].lat), markerPoi);
                    //两点距离小于10米，认为是同一个点
                    if (distance < 10) {
                        t[i].bShow = true;
                        return i;
                    }
                }
            }
           return -1;
        },

        /**
         * 改变路径之后重新起航
         * @param  opt 
         * path 新的轨迹
         * speed 在这段轨迹中的速度
         */
        _resetPath(opt){
            this._path = opt.path;
            this.speed = opt.speed;
            this.i = 0;
        },
        _changePath(opt){
            this._pathIndex = 0;
            this._pathList = opt;
            this._path = opt[0].path;
            this.speed = opt[0].speed;
        }
    });
    /**
     * 自定义的overlay，显示在小车的上方
     * @param {Point} Point 要定位的点.
     * @param {String} html overlay中要显示的东西.
     * @return 无返回值.
     */
    function CustomOverlay(point,html) {
        this._point = point;
        this._html = html;
    }
    CustomOverlay.prototype = new BMap.Overlay();
    CustomOverlay.prototype.initialize = function(map) {
        var div = this._div = baidu.dom.create('div', {style: 'border:solid 1px #ccc;width:auto;min-width:50px;text-align:center;position:absolute;background:#fff;color:#000;font-size:12px;border-radius: 10px;padding:5px;white-space: nowrap;'});
        div.innerHTML = this._html;
        map.getPanes().floatPane.appendChild(div);
        this._map = map;
        return div;
    }
   CustomOverlay.prototype.draw = function() {
        this.setPosition(this.lushuMain._marker.getPosition(), this.lushuMain._marker.getIcon().size);
    }
    baidu.object.extend(CustomOverlay.prototype, {
        //设置overlay的position
        setPosition: function(poi,markerSize) {
            // 此处的bug已修复，感谢 苗冬(diligentcat@gmail.com) 的细心查看和认真指出
            var px = this._map.pointToOverlayPixel(poi),
                styleW = baidu.dom.getStyle(this._div, 'width'),
                styleH = baidu.dom.getStyle(this._div, 'height');
                overlayW = parseInt(this._div.clientWidth || styleW, 10),
                overlayH = parseInt(this._div.clientHeight || styleH, 10);
            this._div.style.left = px.x - overlayW / 2 + 'px';
            this._div.style.bottom = -(px.y - markerSize.height) + 'px';
        },
        //设置overlay的内容
        setHtml: function(html) {
            this._div.innerHTML = html;
        },
        //跟customoverlay相关的实例的引用
        setRelatedClass: function(lushuMain) {
            this.lushuMain = lushuMain;
        }
    });
})();




// var BMapLib=window.BMapLib=BMapLib||{};(function(){var b,a=b=a||{version:"1.5.0"};a.guid="$BAIDU$";(function(){window[a.guid]=window[a.guid]||{};a.dom=a.dom||{};a.dom.g=function(e){if("string"==typeof e||e instanceof String){return document.getElementById(e)}else{if(e&&e.nodeName&&(e.nodeType==1||e.nodeType==9)){return e}}return null};a.g=a.G=a.dom.g;a.lang=a.lang||{};a.lang.isString=function(e){return"[object String]"==Object.prototype.toString.call(e)};a.isString=a.lang.isString;a.dom._g=function(e){if(a.lang.isString(e)){return document.getElementById(e)}return e};a._g=a.dom._g;a.dom.getDocument=function(e){e=a.dom.g(e);return e.nodeType==9?e:e.ownerDocument||e.document};a.browser=a.browser||{};a.browser.ie=a.ie=/msie (\d+\.\d+)/i.test(navigator.userAgent)?(document.documentMode||+RegExp["\x241"]):undefined;a.dom.getComputedStyle=function(f,e){f=a.dom._g(f);var h=a.dom.getDocument(f),g;if(h.defaultView&&h.defaultView.getComputedStyle){g=h.defaultView.getComputedStyle(f,null);if(g){return g[e]||g.getPropertyValue(e)}}return""};a.dom._styleFixer=a.dom._styleFixer||{};a.dom._styleFilter=a.dom._styleFilter||[];a.dom._styleFilter.filter=function(f,j,k){for(var e=0,h=a.dom._styleFilter,g;g=h[e];e++){if(g=g[k]){j=g(f,j)}}return j};a.string=a.string||{};a.string.toCamelCase=function(e){if(e.indexOf("-")<0&&e.indexOf("_")<0){return e}return e.replace(/[-_][^-_]/g,function(f){return f.charAt(1).toUpperCase()})};a.dom.getStyle=function(g,f){var i=a.dom;g=i.g(g);f=a.string.toCamelCase(f);var h=g.style[f]||(g.currentStyle?g.currentStyle[f]:"")||i.getComputedStyle(g,f);if(!h){var e=i._styleFixer[f];if(e){h=e.get?e.get(g):a.dom.getStyle(g,e)}}if(e=i._styleFilter){h=e.filter(f,h,"get")}return h};a.getStyle=a.dom.getStyle;a.dom._NAME_ATTRS=(function(){var e={cellpadding:"cellPadding",cellspacing:"cellSpacing",colspan:"colSpan",rowspan:"rowSpan",valign:"vAlign",usemap:"useMap",frameborder:"frameBorder"};if(a.browser.ie<8){e["for"]="htmlFor";e["class"]="className"}else{e.htmlFor="for";e.className="class"}return e})();a.dom.setAttr=function(f,e,g){f=a.dom.g(f);if("style"==e){f.style.cssText=g}else{e=a.dom._NAME_ATTRS[e]||e;f.setAttribute(e,g)}return f};a.setAttr=a.dom.setAttr;a.dom.setAttrs=function(g,e){g=a.dom.g(g);for(var f in e){a.dom.setAttr(g,f,e[f])}return g};a.setAttrs=a.dom.setAttrs;a.dom.create=function(g,e){var h=document.createElement(g),f=e||{};return a.dom.setAttrs(h,f)};a.object=a.object||{};a.extend=a.object.extend=function(g,e){for(var f in e){if(e.hasOwnProperty(f)){g[f]=e[f]}}return g}})();var c=BMapLib.LuShu=function(g,f,e){if(!f||f.length<1){return}this._map=g;this._path=f;this.i=0;this._setTimeoutQuene=[];this._projection=this._map.getMapType().getProjection();this._opts={icon:null,speed:4000,defaultContent:""};this._setOptions(e);this._rotation=0;if(!this._opts.icon instanceof BMap.Icon){this._opts.icon=defaultIcon}};c.prototype._setOptions=function(e){if(!e){return}for(var f in e){if(e.hasOwnProperty(f)){this._opts[f]=e[f]}}};c.prototype.start=function(){var f=this,e=f._path.length;if(f.i&&f.i<e-1){if(!f._fromPause){return}else{if(!f._fromStop){f._moveNext(++f.i)}}}else{f._addMarker();f._timeoutFlag=setTimeout(function(){f._addInfoWin();if(f._opts.defaultContent==""){f.hideInfoWindow()}f._moveNext(f.i)},400)}this._fromPause=false;this._fromStop=false},c.prototype.stop=function(){this.i=0;this._fromStop=true;clearInterval(this._intervalFlag);this._clearTimeout();for(var g=0,f=this._opts.landmarkPois,e=f.length;g<e;g++){f[g].bShow=false}};c.prototype.pause=function(){clearInterval(this._intervalFlag);this._fromPause=true;this._clearTimeout()};c.prototype.hideInfoWindow=function(){this._overlay._div.style.visibility="hidden"};c.prototype.showInfoWindow=function(){this._overlay._div.style.visibility="visible"};a.object.extend(c.prototype,{_addMarker:function(f){if(this._marker){this.stop();this._map.removeOverlay(this._marker);clearTimeout(this._timeoutFlag)}this._overlay&&this._map.removeOverlay(this._overlay);var e=new BMap.Marker(this._path[0]);this._opts.icon&&e.setIcon(this._opts.icon);this._map.addOverlay(e);this._marker=e},_addInfoWin:function(){var f=this;var e=new d(f._marker.getPosition(),f._opts.defaultContent);e.setRelatedClass(this);this._overlay=e;this._map.addOverlay(e)},_getMercator:function(e){return this._map.getMapType().getProjection().lngLatToPoint(e)},_getDistance:function(f,e){return Math.sqrt(Math.pow(f.x-e.x,2)+Math.pow(f.y-e.y,2))},_move:function(n,j,m){var i=this,h=0,e=10,f=this._opts.speed/(1000/e),l=this._projection.lngLatToPoint(n),k=this._projection.lngLatToPoint(j),g=Math.round(i._getDistance(l,k)/f);if(g<1){i._moveNext(++i.i);return}i._intervalFlag=setInterval(function(){if(h>=g){clearInterval(i._intervalFlag);if(i.i>i._path.length){return}i._moveNext(++i.i)}else{h++;var o=m(l.x,k.x,h,g),r=m(l.y,k.y,h,g),q=i._projection.pointToLngLat(new BMap.Pixel(o,r));if(h==1){var p=null;if(i.i-1>=0){p=i._path[i.i-1]}if(i._opts.enableRotation==true){i.setRotation(p,n,j)}if(i._opts.autoView){if(!i._map.getBounds().containsPoint(q)){i._map.setCenter(q)}}}i._marker.setPosition(q);i._setInfoWin(q)}},e)},setRotation:function(l,f,m){var j=this;var e=0;f=j._map.pointToPixel(f);m=j._map.pointToPixel(m);if(m.x!=f.x){var k=(m.y-f.y)/(m.x-f.x),g=Math.atan(k);e=g*360/(2*Math.PI);if(m.x<f.x){e=-e+90+90}else{e=-e}j._marker.setRotation(-e)}else{var h=m.y-f.y;var i=0;if(h>0){i=-1}else{i=1}j._marker.setRotation(-i*90)}return},linePixellength:function(f,e){return Math.sqrt(Math.abs(f.x-e.x)*Math.abs(f.x-e.x)+Math.abs(f.y-e.y)*Math.abs(f.y-e.y))},pointToPoint:function(f,e){return Math.abs(f.x-e.x)*Math.abs(f.x-e.x)+Math.abs(f.y-e.y)*Math.abs(f.y-e.y)},_moveNext:function(e){var f=this;if(e<this._path.length-1){f._move(f._path[e],f._path[e+1],f._tween.linear)}},_setInfoWin:function(g){var f=this;if(!f._overlay){return}f._overlay.setPosition(g,f._marker.getIcon().size);var e=f._troughPointIndex(g);if(e!=-1){clearInterval(f._intervalFlag);f._overlay.setHtml(f._opts.landmarkPois[e].html);f._overlay.setPosition(g,f._marker.getIcon().size);f._pauseForView(e)}else{f._overlay.setHtml(f._opts.defaultContent)}},_pauseForView:function(e){var g=this;var f=setTimeout(function(){g._moveNext(++g.i)},g._opts.landmarkPois[e].pauseTime*1000);g._setTimeoutQuene.push(f)},_clearTimeout:function(){for(var e in this._setTimeoutQuene){clearTimeout(this._setTimeoutQuene[e])}this._setTimeoutQuene.length=0},_tween:{linear:function(f,j,h,i){var e=f,l=j-f,g=h,k=i;return l*g/k+e}},_troughPointIndex:function(f){var h=this._opts.landmarkPois,j;for(var g=0,e=h.length;g<e;g++){if(!h[g].bShow){j=this._map.getDistance(new BMap.Point(h[g].lng,h[g].lat),f);if(j<10){h[g].bShow=true;return g}}}return -1}});function d(e,f){this._point=e;this._html=f}d.prototype=new BMap.Overlay();d.prototype.initialize=function(e){var f=this._div=a.dom.create("div",{style:"border:solid 1px #ccc;width:auto;min-width:50px;text-align:center;position:absolute;background:#fff;color:#000;font-size:12px;border-radius: 10px;padding:5px;white-space: nowrap;"});f.innerHTML=this._html;e.getPanes().floatPane.appendChild(f);this._map=e;return f};d.prototype.draw=function(){this.setPosition(this.lushuMain._marker.getPosition(),this.lushuMain._marker.getIcon().size)};a.object.extend(d.prototype,{setPosition:function(h,i){var f=this._map.pointToOverlayPixel(h),e=a.dom.getStyle(this._div,"width"),g=a.dom.getStyle(this._div,"height");overlayW=parseInt(this._div.clientWidth||e,10),overlayH=parseInt(this._div.clientHeight||g,10);this._div.style.left=f.x-overlayW/2+"px";this._div.style.bottom=-(f.y-i.height)+"px"},setHtml:function(e){this._div.innerHTML=e},setRelatedClass:function(e){this.lushuMain=e}})})();