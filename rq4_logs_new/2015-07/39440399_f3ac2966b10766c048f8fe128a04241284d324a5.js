/**
 * 拖动类库
 * User: MrCo
 * Date: 13-7-11
 * Time: 上午11:03
 */
(function(w){
    var $ = function(id){
        return document.getElementById(id);
    }

    /*
    * 事件注册器
    * @element 需要注册的DOM元素
    * @eventName 注册的事件类型名称
    * @handler 注册事件需要处理的回调方法 */
    var on = function(element,eventName,handler){
        if(element.addEventListener){
            element.addEventListener(eventName,handler,false);
        }else if(element.attachEvent){
            element.attachEvent('on' + eventName,handler);
        }else{
            element['on' + eventName] = handler;
        }
    }

    /*
    * 拖动类库主体*/
    var Drag = function(){

    }

    on($('drag'),'mousedown',function(){

    })

    on($('drag'),'mouseup',function(){

    })
})(window)