/**
 * widget manager
 * @author  ddchen chenjunyu@baidu.com
 */
window.WidgetManager = (function($, listener) {

    var WIDGET_SPECIAL_CHANNEL = "widget_special_channel";
    var WIDGET_INITIAL_TYPE = "widget_initial_type";

    var widgetMap = {};

    var collect = function(widgetInstance) {
        var el = widgetInstance.el;
        if (!el) return;
        if (widgetMap[el] && console && console.log) {
            console.log("define the same el!");
        }
        widgetMap[el] = widgetInstance;
        // initialize widget
        listener.trigger(WIDGET_SPECIAL_CHANNEL, WIDGET_INITIAL_TYPE, {
            el: el
        });
    }

    var ready = function(url, callback) {
        if (!url) return;
        var parts = url.split("?");
        var url = parts[0];
        if (widgetMap[url]) { // do something immediately
            callback && callback();
        } else { // wait
            listener.on(WIDGET_SPECIAL_CHANNEL, WIDGET_INITIAL_TYPE, function(type, data) {
                var el = data.el;
                if (el == url) {
                    callback && callback();
                }
            });
        }
    }

    var findWidget = function(url) {
        var parts = url.split("?");
        var el = parts[0];
        var widgetInstance = widgetMap[el];
        return widgetInstance;
    }

    var getFuncName = function(url) {
        var parts = url.split("?");
        var el = parts[0];
        var funcName = url.substring(el.length + 1, url.length);
        if (!funcName) {
            throw Error("missing widget function name.");
        }
        if (funcName[0] == "_") {
            throw Error("cannot call widget function which name started with _.");
        }
        return funcName;
    }

    /**
     * options
     *       url   like el?fun
     *       data  array
     *       success
     */
    var communicate = function(options) {
        var url = options.url;
        ready(url, function() {
            var widgetInstance = findWidget(url);
            var funcName = getFuncName(url);
            var func = widgetInstance[funcName];
            if (!func) {
                options.miss && options.miss(1);
            }
            var data = options.data;
            if (!$.isArray(data)) {
                data = [data];
            }
            var result = func.apply(widgetInstance, data);
            options.success && options.success(result);
        });
    }

    return {
        collect: collect,
        communicate: communicate,
        ready: ready
    }
})($, listener);

/**
 * @fileoverview  window.Widget是组件基类
 * window.Widget是基类的构造函数，有一个静态方法extend用于扩展组件，extend方法返回组件构造函数，window.Widget形如：
 * window.Widget = function(pageName) {
 * 		_init(pageName);
 * }
 * window.Widget.extend = function() {}
 * @author xiaole@baidu.com
 * @date 2013/1/25
 *
 * @update by ddchen chenjunyu@baidu.com
 *
 * @standard
 *     attribute of widget started with "_" is private attibutes
 */
window.Widget = (function($, listener, WidgetManager) {
    var self;

    /**
     * define the widget class level plugin mechanism
     * expose extentions
     */
    var widgetCommonActionMap = {};

    var _init = function(pageName) {
        self.$el = $(self.el);
        _paeseDOM();
        if (self.init && $.isFunction(self.init)) {
            self.init(pageName);
        }
        _bind();
    };
    //TO-DO
    var _paeseDOM = function() {
        self.$dom = {};
        var html = self.$el.html() || '';
        var selectors = html.match(/data-node="([^"]*)"/img) || [];
        var item, key;
        for (var i = selectors.length - 1; i >= 0; i--) {
            item = selectors[i];
            key = item.split('=')[1].replace(/"/g, '');
            self.$dom[key] = self.$el.find('[' + item + ']');
        };
    };
    var _bind = function() {
        var events = self.events,
            channels = self.channels;
        var bindEventSplitter = /^(\S+)\s*(.*)$/;
        var eventName, selector, channelName, match;
        if (events && events instanceof Object) {
            $.each(events, function(key, method) {
                if (!$.isFunction(method)) {
                    method = self[method];
                }
                if (!method) {
                    return true; //true类似continue,false类似break
                }
                match = key.match(bindEventSplitter);
                eventName = match[1];
                selector = match[2];
                _bindBrowserEvent(eventName, selector, method);
            });
        }
        if (channels && channels instanceof Object) {
            $.each(channels, function(key, method) {
                if (!$.isFunction(method)) {
                    method = self[method];
                }
                if (!method) {
                    return true; //true类似continue,false类似break
                }
                match = key.match(bindEventSplitter);
                channelName = match[1];
                eventName = match[2];
                _bindCustomerEvent(channelName, eventName, method);
            });
        }
    };
    /**
     * 浏览器事件绑定
     * @param  {String} eventName 事件名，如click等
     * @param  {String} selector  出发事件的元素
     * @param  {function} method  事件处理函数
     */
    var _bindBrowserEvent = function(eventName, selector, method) {
        var el = self.el || 'body';
        if (selector) {
            $(el).on(eventName, selector, $.proxy(method, self));
        } else {
            $(el).on(eventName, $.proxy(method, self));
        }
    };
    /**
     * 绑定广播事件
     * @param  {string} eventName 事件触发的频道，如common.page
     * @param  {string} selector  自定义事件名称，如switchstart等
     * @param  {function} method  事件处理函数
     */
    var _bindCustomerEvent = function(channelName, eventName, method) {
        listener.on(channelName, eventName, $.proxy(method, self));
    };

    var combineObject = function(defaultObject, extendObject) {
        if (!extendObject) extendObject = {};
        if (!defaultObject) {
            return extendObject;
        }
        for (var name in defaultObject) {
            if (!extendObject[name]) {
                extendObject[name] = defaultObject[name];
            }
        }
        return extendObject;
    }

    function Widget(pageName) {
        _init(pageName);
    }

    /**
     * 根据传进来的对象实例扩展组件基类，会返回child而不是Widget是因为如果在Widget基类上直接扩展，多个组件会相互影响
     * @param  {Object} obj 组件对象实例
     * @return {function}  扩展Widget基类后的组件构造函数
     */
    Widget.extend = function(obj) {
        var parent = this;
        var child = function() {
            self = this;
            return parent.apply(this, arguments);
        };
        var Surrogate = function() {
            this.constructor = child;
        };
        Surrogate.prototype = parent.prototype;
        child.prototype = new Surrogate();
        // add defaults
        child.prototype = combineObject(widgetCommonActionMap, child.prototype);
        Surrogate = null;
        $.extend(child.prototype, obj);
        child.createWidget = function(pageName) {
            var childInstance = new child(pageName);
            // collect widget
            WidgetManager.collect(childInstance);
            return childInstance;
        }
        return child;
    };

    Widget.register = function(propName, value) {
        if (widgetCommonActionMap[propName] && console && console.log) {
            console.log("register same name widget plugin!");
        }
        widgetCommonActionMap[propName] = value;
    }
    return Widget;
})($, listener, WidgetManager);