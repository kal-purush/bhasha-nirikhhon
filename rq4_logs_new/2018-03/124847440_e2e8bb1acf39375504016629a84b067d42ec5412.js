(function(factory){
    if(typeof define === 'function' && define.amd){
        define(['jQuery'], factory);
    }else if(typeof exports === 'object'){
        factory(require('jQuery'));
    }else{
        factory(jQuery)
    }
})(function($){
    "use strict";
    function Miniview(option, $container){
        var defaultOption = {
            node: '.item',
            container: '#canvasWrapper',
            miniW: 150,
            miniH: 150
        };
        this.option = $.extend(true, {}, defaultOption, option);
        this.$view = $container;
        this.$port = $('<div class="port"></div>');
        this.$nodebox = $('<div class="view"><ul></ul></div>');
        this.$view.css({
            width: this.option.miniW + 'px',
            height: this.option.miniH + 'px',
            padding: '5px'
        });
        this.$view.append(this.$port);
        this.$view.append(this.$nodebox);
        this.init();
    }

    Miniview.prototype = {
        init: function(){
            var that = this;
            //缓存边缘四个点
            that.nodeL = {};
            that.nodeR = {};
            that.nodeT = {};
            that.nodeB = {};
            that.refresh();
            that.events();
        },

        //确定画布边缘四个点，并渲染miniview
        refresh: function(){
            var that = this,
                $nodes = $(that.option.node),
                nodeArr = [],
                viewInfo = {},
                $miniElemContainer = that.$nodebox.find('ul');
            $miniElemContainer.empty();
            $.each($nodes,function(key,item){

                var offsetL = $(item)[0].offsetLeft,
                    offsetT = $(item)[0].offsetTop,
                    nodeId = $(item)[0].id;
                
                nodeArr.push({
                    left: offsetL,
                    top: offsetT,
                    id: nodeId
                });
            });
            viewInfo = miniview.core.getViewPoint(nodeArr);
            that.nodeL = viewInfo.nodeL;
            that.nodeR = viewInfo.nodeR;
            that.nodeT = viewInfo.nodeT;
            that.nodeB = viewInfo.nodeB;
            var scaleDiffW,
                scaleDiffH,
                miniW = that.$nodebox.width(),
                miniH = that.$nodebox.height(),
                miniScale,
                nodeW = $nodes.width(),
                nodeH = $nodes.height(),
                canvasW = $(that.option.container).width(),
                canvasH = $(that.option.container).height(),
                nodeGapX = (that.nodeR && that.nodeL) && (that.nodeR.left - that.nodeL.left + nodeW),
                nodeGapY = (that.nodeB && that.nodeT) && (that.nodeB.top - that.nodeT.top + nodeH),
                miniNodeW = 8,
                miniNodeH = 8,
                miniFlag = false;
            
            //获取画布点范围rect
            scaleDiffW = (that.nodeL && that.nodeR) && nodeGapX;
            scaleDiffH = (that.nodeT && that.nodeB) && nodeGapY;
            miniFlag = scaleDiffW > scaleDiffH;

            //获取miniview显示比例
            that.option.miniScale = miniScale = miniFlag ? miniW / scaleDiffW : miniH / scaleDiffH;

            //根据比例确定miniview点大小
            miniNodeW = parseInt(nodeW * miniScale);
            miniNodeH = parseInt(nodeH * miniScale);
            that.option.nodeSize = {
                w: miniNodeW,
                h: miniNodeH
            };
            //居中显示nodebox
            $miniElemContainer.css({
                width: parseInt(miniScale * scaleDiffW) + 'px',
                height: parseInt(miniScale * scaleDiffH) + 'px',
                marginLeft: parseInt(- miniScale * scaleDiffW / 2) + 'px',
                marginTop: parseInt(-miniScale * scaleDiffH / 2) + 'px'
            });

            var subtractL = that.nodeL && that.nodeL.left,
                subtractT = that.nodeT && that.nodeT.top;
            //渲染miniview上的点
            for(var i = 0; i < nodeArr.length; i++){
                var $node = $('<li class="miniview-element"></li>'),
                    nodeItem = nodeArr[i],
                    tmpL = parseInt((nodeItem.left -subtractL) * miniScale),
                    tmpT = parseInt((nodeItem.top - subtractT) * miniScale);
                $node.css({
                    width: miniNodeW + 'px',
                    height: miniNodeH + 'px',
                    left: tmpL + 'px',
                    top: tmpT + 'px'
                });
                $node.attr('id','mini' + nodeItem.id);
                $miniElemContainer.append($node);

                nodeItem.id == that.nodeL.id && (that.miniNodeL = $node);
                nodeItem.id == that.nodeR.id && (that.miniNodeR = $node);
                nodeItem.id == that.nodeT.id && (that.miniNodeT = $node);
                nodeItem.id == that.nodeB.id && (that.miniNodeB = $node);

            }

            //记录可视区与范围比例
            var $containL = $miniElemContainer[0].offsetLeft,
                $containT = $miniElemContainer[0].offsetTop,
                miniPadding = 5,
                miniGap = {             //记录点的相对位置
                    left: miniPadding + $containL,
                    top: miniPadding + $containT
                };
            that.$port.css({
                width: parseInt(canvasW * miniScale) + 'px',
                height: parseInt(canvasH * miniScale) + 'px',
                left: parseInt(miniGap.left - subtractL * miniScale) + 'px',
                top: parseInt(miniGap.top - subtractT * miniScale) + 'px'
            }).css('cursor','-webkit-grab');

            that.moveRange = {          //确认port范围
                left: that.miniNodeL && (that.miniNodeL[0].offsetLeft + miniGap.left + that.option.nodeSize.w) - that.$port.width(),
                right: that.miniNodeR && that.miniNodeR[0].offsetLeft + miniGap.left,
                top: that.miniNodeT && (that.miniNodeT[0].offsetTop + miniGap.top + that.option.nodeSize.h) - that.$port.height(),
                bottom: that.miniNodeB && that.miniNodeB[0].offsetTop + miniGap.top
            };

            that.originPort = {     //确定port位置
                x: that.$port[0].offsetLeft,
                y: that.$port[0].offsetTop
            };

        },

        //miniview事件管理
        events: function(){
            var that = this,
                $port = that.$port, //移动对象
                posParams = {
                    left: $port[0].offsetLeft,
                    top: $port[0].offsetTop,
                    currentX: 0,
                    currentY: 0,
                    flag: false
                },
                $container = $(that.option.container).find('#canvas');
            $port.on('mousedown',function(ev){
                posParams.flag = true;
                posParams.currentX = ev.clientX;
                posParams.currentY = ev.clientY;
            });

            $(document).on('mouseup', function (ev) {
                posParams.flag = false;
                posParams.left = $port[0].offsetLeft;
                posParams.top = $port[0].offsetTop;
            });

            $(document).on('mousemove',function(ev){
                var miniGap = that.miniGap,
                    moveRange = that.moveRange,
                    originPort = that.originPort;
                if(!posParams.flag){
                    return;
                }
                var disX = ev.clientX - posParams.currentX,
                    disY = ev.clientY - posParams.currentY,
                    portL = posParams.left + disX,
                    portT = posParams.top + disY;
                
                $port.css({
                    left: parseInt(portL < moveRange.left ? moveRange.left : (portL > moveRange.right ? moveRange.right : portL)) + 'px',
                    top: parseInt(portT < moveRange.top ? moveRange.top : (portT > moveRange.bottom ? moveRange.bottom : portT)) + 'px'
                });

                $container.css({
                    left: -($port[0].offsetLeft - originPort.x) / that.option.miniScale + 'px',
                    top: -($port[0].offsetTop - originPort.y) / that.option.miniScale + 'px'
                });

                ev.preventDefault();
            });

            $(window).on('resize',function(ev){
                that.refresh();
            });

        }

    };

    var miniview = {
        core: {
            //确定边缘的四个点
            getViewPoint: function(nodeArr){
                var that = this,
                    maxminL = $.extend(true, [], nodeArr),
                    maxminT = $.extend(true, [], nodeArr);
                
                for(var i = 1; i < nodeArr.length; i++){
                    var nodeL = nodeArr[i].left,
                        nodeT = nodeArr[i].top,
                        nodeId = nodeArr[i].id;
                    (function(i){
                        for(var t = i - 1; t >= 0; t--){
                            var tempT = maxminT[t].top;
                            if(tempT > nodeT){
                                maxminT[t + 1] = maxminT[t];
                            }else{
                                break;
                            }
                        }
                        maxminT[t + 1] = nodeArr[i];
                    })(i);

                    (function (i) {
                        for (var t = i - 1; t >= 0; t--) {
                            var tempL = maxminL[t].left;
                            if (tempL > nodeL) {
                                maxminL[t + 1] = maxminL[t];
                            } else {
                                break;
                            }
                        }
                        maxminL[t + 1] = nodeArr[i];
                    })(i);
                }
                //缓存边缘四个点
                return{
                    nodeL: maxminL[0],
                    nodeR: maxminL[nodeArr.length - 1],
                    nodeT: maxminT[0],
                    nodeB: maxminT[nodeArr.length - 1]
                };
            }
        }
    };

    $.fn.miniview = function(option){
        var $container = this,
            args = arguments,
            result;
        this.each(function(){
            var data = $container.data('miniview');
            if(!data && typeof option === 'string'){
                throw new Error('Miniview is uninitialized.');
            }else if(data && typeof option === 'object'){
                throw new Error('Miniview is initialized.');
            }else if(!data && (typeof option === 'object' || typeof option === 'undefined')){
                $container.empty().data('miniview', new Miniview(option,$container));
            }else if(data && typeof option === 'string'){
                data[option].apply(data, Array.prototype.slice.call(args,1));
            }
        });
        return typeof result === 'undefined' ? $container : result;
    }
})