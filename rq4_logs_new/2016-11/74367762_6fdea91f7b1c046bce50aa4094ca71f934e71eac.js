(function(){
    window.AutomizyGlobalPlugins = window.AutomizyGlobalPlugins || {i:0};
    window.AutomizyGlobalZIndex = window.AutomizyGlobalZIndex || 2000;
    window.AutomizyGridSystem = window.$AGS = new function () {
        var t = this;
        t.version = '0.1.1';
        t.elements = {};
        t.dialogs = {};
        t.inputs = {};
        t.buttons = {};
        t.forms = {};
        t.functions = {};
        t.xhr = {};
        t.config = {
            dir:'.',
            url:'https://app.automizy.com'
        };
        t.m = {};
        t.d = {};
    }();
    return $AGS;
})();

(function(){

    $AGS.runTheFunctions = function(functions, thisParameter, parameters){
        var functions = functions || [];
        var thisParameter = thisParameter || $AGS;
        var parameters = parameters || [];
        for(var i = 0; i < functions.length; i++) {
            functions[i].apply(thisParameter, parameters);
        }
    };

})();

(function(){

    $AGS.functions.pluginsLoadedFunctions = [];
    $AGS.pluginsLoaded = function(f){
        if(typeof f === 'function'){
            $AGS.functions.pluginsLoadedFunctions.push(f);
            if($AGS.automizyPluginsLoaded){
                f.apply($AGS, []);
            }
            return $AGS;
        }
        $AGS.runTheFunctions($AGS.functions.pluginsLoadedFunctions, $AGS, []);
        $AGS.automizyPluginsLoaded = true;
        return $AGS;
    };

})();

(function(){
    $AGS.loadPlugins = function () {
        (function () {
            if (typeof window.jQuery === 'undefined') {
                var script = document.createElement("SCRIPT");
                script.src = $AGS.config.dir + "/vendor/jquery/jquery.min.js";
                script.type = 'text/javascript';
                document.getElementsByTagName("head")[0].appendChild(script);
            }
            var checkReady = function (callback) {
                if (typeof window.jQuery === 'function') {
                    callback(jQuery);
                } else {
                    window.setTimeout(function () {
                        checkReady(callback);
                    }, 100);
                }
            };

            checkReady(function ($) {
                $AGS.pluginsLoaded();
            });

        })();
    };
})();

(function(){
    $AGS.init = function () {
        if(typeof $AGS.automizyInited === 'undefined'){
            $AGS.automizyInited = false;
        }

        if(!$AGS.automizyInited){
            $AGS.automizyInited = true;
            $AGS.loadPlugins();
        }

        return $AGS;
    };
})();

(function(){

    $AGS.functions.layoutReadyFunctions = [];
    $AGS.layoutReady = function(f){
        if(typeof f === 'function') {
            $AGS.functions.layoutReadyFunctions.push(f);
            if($AGS.automizyLayoutReady){
                f.apply($AGS, []);
            }
            return $AGS;
        }
        $AGS.runTheFunctions($AGS.functions.layoutReadyFunctions);
        $AGS.automizyLayoutReady = true;
        return $AGS;
    };

})();

(function(){

    $AGS.functions.readyFunctions = [];
    $AGS.ready = function(f){
        if(typeof f === 'function') {
            $AGS.functions.readyFunctions.push(f);
            if($AGS.automizyReady){
                f.apply($AGS, []);
            }
            return $AGS;
        }
        $AGS.runTheFunctions($AGS.functions.readyFunctions);
        $AGS.automizyReady = true;
        return $AGS;
    };

})();

(function(){
    $AGS.pluginsLoaded(function () {

        $AGS.$tmp = $('<div id="automizy-grid-system-tmp"></div>');

        $AGS.layoutReady();
        $AGS.ready();
    });
})();

(function(){
    var Container = function () {
        var t = this;
        t.d = {
            fluid:true
        };

        t.d.$widget = $('<div class="ags-container ags-container-fluid"></div>');

    };

    var p = Container.prototype;

    p.addRow = function(row){
        return this.addRows([row]);
    };
    p.rows = p.addRows = function(rows){
        var t = this;
        var rows = rows || [];
        for(var i = 0; i < rows.length; i++){
            if(rows[i] instanceof $AGS.m.Row){
                rows[i].drawTo(t.d.$widget);
            }else if(typeof rows[i] === 'array' || typeof rows[i] === 'object'){
                $AGS.newContainer(rows[i]).drawTo(t.d.$widget);
            }
        }
        return t;
    };
    p.fluid = function(fluid){
        var t = this;
        if (typeof fluid !== 'undefined') {
            t.d.fluid = !!fluid;
            if(t.d.fluid){
                t.widget().addClass('ags-container-fluid');
            }else{
                t.widget().removeClass('ags-container-fluid');
            }
            return t;
        }
        return t.d.fluid;
    };

    p.content = function (content) {
        var t = this;
        if (typeof content !== 'undefined') {
            if (t.d.$widget.contents() instanceof jQuery) {
                t.d.$widget.contents().appendTo($AGS.$tmp);
            }
            t.d.$widget.empty();
            t.d.content = content;
            if (t.d.content instanceof jQuery) {
                t.d.content.appendTo(t.d.$widget);
            } else if(typeof t.d.content.drawTo === 'function') {
                t.d.content.drawTo(t.d.$widget);
            } else {
                t.d.$widget.html(t.d.content);
            }
            return t;
        }
        return t.d.content;
    };
    p.widget = function () {
        return this.d.$widget;
    };
    p.draw = p.drawTo = function (target) {
        var t = this;
        var target = target || $('body').eq(0);
        if(typeof target.widget === 'function') {
            t.d.$widget.appendTo(target.widget());
        } else{
            t.d.$widget.appendTo(target);
        }
        return t;
    };

    $AGS.m.Container = Container;
    $AGS.newContainer = function () {
        return new $AGS.m.Container();
    };

    return $AGS.m.Container;
})();

(function(){
    var Row = function () {
        var t = this;
        t.d = {

        };

        t.d.$widget = $('<div class="ags-row"></div>');

    };

    var p = Row.prototype;

    p.addCol = function(col){
        return this.addCols([col]);
    };
    p.cols = p.addCols = function(cols){
        var t = this;
        var cols = cols || [];
        for(var i = 0; i < cols.length; i++){
            if(cols[i] instanceof $AGS.m.Col){
                cols[i].drawTo(t.d.$widget);
            }else if(typeof cols[i] === 'array' || typeof cols[i] === 'object'){
                $AGS.newRow(cols[i]).drawTo(t.d.$widget);
            }
        }
        return t;
    };

    p.content = function (content) {
        var t = this;
        if (typeof content !== 'undefined') {
            if (t.d.$widget.contents() instanceof jQuery) {
                t.d.$widget.contents().appendTo($AGS.$tmp);
            }
            t.d.$widget.empty();
            t.d.content = content;
            if (t.d.content instanceof jQuery) {
                t.d.content.appendTo(t.d.$widget);
            } else if(typeof t.d.content.drawTo === 'function') {
                t.d.content.drawTo(t.d.$widget);
            } else {
                t.d.$widget.html(t.d.content);
            }
            return t;
        }
        return t.d.content;
    };
    p.widget = function () {
        return this.d.$widget;
    };
    p.draw = p.drawTo = function (target) {
        var t = this;
        var target = target || $('body').eq(0);
        if(typeof target.widget === 'function') {
            t.d.$widget.appendTo(target.widget());
        } else{
            t.d.$widget.appendTo(target);
        }
        return t;
    };

    $AGS.m.Row = Row;
    $AGS.newRow = function () {
        return new $AGS.m.Row();
    };

    return $AGS.m.Row;
})();

(function(){
    var Col = function () {
        var t = this;
        t.d = {
            xs: {
                size: 12,
                offset: false,
                pull: false,
                push: false,
                hidden: false
            },
            sm: {
                size: 12,
                offset: false,
                pull: false,
                push: false,
                hidden: false
            },
            md: {
                size: 12,
                offset: false,
                pull: false,
                push: false,
                hidden: false
            },
            lg: {
                size: 12,
                offset: false,
                pull: false,
                push: false,
                hidden: false
            }
        };

        t.d.$widget = $('<div class="ags-col"></div>');

    };

    var p = Col.prototype;

    p.reClassType = function (type) {
        var t = this;
        if (t.d[type].size !== 0) {
            t.d.$widget.addClass('ags-col-' + type + ' ags-col-' + type + '-' + t.d[type].size);
        }
        if (t.d[type].offset !== false) {
            t.d.$widget.addClass('ags-col-' + type + ' ags-col-' + type + '-offset-' + t.d[type].offset);
        }
        if (t.d[type].pull !== false) {
            t.d.$widget.addClass('ags-col-' + type + ' ags-col-' + type + '-pull-' + t.d[type].pull);
        }
        if (t.d[type].push !== false) {
            t.d.$widget.addClass('ags-col-' + type + ' ags-col-' + type + '-push-' + t.d[type].push);
        }
        if (t.d[type].hidden !== false) {
            t.d.$widget.addClass('ags-col-' + type + ' ags-col-hidden-' + type);
        }
        return t;
    };

    p.reClass = function () {
        var t = this;
        t.d.$widget.removeClass(function (index, css) {
            return (css.match(/(^|\s)ags-col-\S+/g) || []).join(' ');
        });

        t.reClassType('xs');
        t.reClassType('sm');
        t.reClassType('md');
        t.reClassType('lg');

        return t;
    };

    p.xs = p.extraSmall = function (value) {
        var t = this;
        if (typeof value !== 'undefined') {
            if (typeof value === 'object' || typeof value === 'array') {
                if (typeof value.size !== 'undefined') {
                    t.d.xs.size = parseInt(value.size);
                }
                if (typeof value.offset !== 'undefined') {
                    t.d.xs.offset = parseInt(value.offset);
                }
                if (typeof value.pull !== 'undefined') {
                    t.d.xs.pull = parseInt(value.pull);
                }
                if (typeof value.push !== 'undefined') {
                    t.d.xs.push = parseInt(value.push);
                }
                if (typeof value.hidden !== 'undefined') {
                    t.d.xs.hidden = !!value.hidden;
                }
            } else {
                t.d.xs.size = value;
            }
            t.reClass();
            return t;
        }
        return t.d.xs;
    };
    p.sm = p.small = function (value) {
        var t = this;
        if (typeof value !== 'undefined') {
            if (typeof value === 'object' || typeof value === 'array') {
                if (typeof value.size !== 'undefined') {
                    t.d.sm.size = parseInt(value.size);
                }
                if (typeof value.offset !== 'undefined') {
                    t.d.sm.offset = parseInt(value.offset);
                }
                if (typeof value.pull !== 'undefined') {
                    t.d.sm.pull = parseInt(value.pull);
                }
                if (typeof value.push !== 'undefined') {
                    t.d.sm.push = parseInt(value.push);
                }
                if (typeof value.hidden !== 'undefined') {
                    t.d.sm.hidden = !!value.hidden;
                }
            } else {
                t.d.sm.size = value;
            }
            t.reClass();
            return t;
        }
        return t.d.sm;
    };
    p.md = p.medium = function (value) {
        var t = this;
        if (typeof value !== 'undefined') {
            if (typeof value === 'object' || typeof value === 'array') {
                if (typeof value.size !== 'undefined') {
                    t.d.md.size = parseInt(value.size);
                }
                if (typeof value.offset !== 'undefined') {
                    t.d.md.offset = parseInt(value.offset);
                }
                if (typeof value.pull !== 'undefined') {
                    t.d.md.pull = parseInt(value.pull);
                }
                if (typeof value.push !== 'undefined') {
                    t.d.md.push = parseInt(value.push);
                }
                if (typeof value.hidden !== 'undefined') {
                    t.d.md.hidden = !!value.hidden;
                }
            } else {
                t.d.md.size = value;
            }
            t.reClass();
            return t;
        }
        return t.d.md;
    };
    p.lg = p.large = function (value) {
        var t = this;
        if (typeof value !== 'undefined') {
            if (typeof value === 'object' || typeof value === 'array') {
                if (typeof value.size !== 'undefined') {
                    t.d.lg.size = parseInt(value.size);
                }
                if (typeof value.offset !== 'undefined') {
                    t.d.lg.offset = parseInt(value.offset);
                }
                if (typeof value.pull !== 'undefined') {
                    t.d.lg.pull = parseInt(value.pull);
                }
                if (typeof value.push !== 'undefined') {
                    t.d.lg.push = parseInt(value.push);
                }
                if (typeof value.hidden !== 'undefined') {
                    t.d.lg.hidden = !!value.hidden;
                }
            } else {
                t.d.lg.size = value;
            }
            t.reClass();
            return t;
        }
        return t.d.lg;
    };

    p.content = function (content) {
        var t = this;
        if (typeof content !== 'undefined') {
            if (t.d.$widget.contents() instanceof jQuery) {
                t.d.$widget.contents().appendTo($AGS.$tmp);
            }
            t.d.$widget.empty();
            t.d.content = content;
            if (t.d.content instanceof jQuery) {
                t.d.content.appendTo(t.d.$widget);
            } else if(typeof t.d.content.drawTo === 'function') {
                t.d.content.drawTo(t.d.$widget);
            } else {
                t.d.$widget.html(t.d.content);
            }
            return t;
        }
        return t.d.content;
    };
    p.widget = function () {
        return this.d.$widget;
    };
    p.draw = p.drawTo = function (target) {
        var t = this;
        var target = target || $('body').eq(0);
        if(typeof target.widget === 'function') {
            t.d.$widget.appendTo(target.widget());
        } else{
            t.d.$widget.appendTo(target);
        }
        return t;
    };

    $AGS.m.Col = Col;
    $AGS.newCol = function () {
        return new $AGS.m.Col();
    };

    return $AGS.m.Col;
})();

(function(){
    console.log('%c AutomizyGridSystem loaded! ', 'background: #000000; color: #bada55; font-size:14px');
})();

(function(){})();