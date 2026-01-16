// ==UserScript==
// @name Douban Sofa Machine
// @namespace http://ligeleng.net/
// @version 0.1
// @description Douban Sofa
// @author LGL
// @include /^https?:\/\/(www\.)?douban\.com(\/\w+(\/)?)+\?(debug=(\w+))\w*/
// @require https://unpkg.com/react@15/dist/react.js
// @require https://unpkg.com/react-dom@15/dist/react-dom.js
// @require https://unpkg.com/babel-standalone@6/babel.min.js
// @resource layoutCss https://gist.githubusercontent.com/p6vital/4ec0414834bc0fd43b1140cb4c1a8570/raw
// @resource reactJsx https://gist.githubusercontent.com/p6vital/8ca3fb3c2b22ddb757e53bde334e7917/raw
// @grant GM_addStyle
// @grant GM_getResourceText
// ==/UserScript==

(function() {
    'use strict';

    // Inject CSS
    var layoutCss = GM_getResourceText("layoutCss");
    GM_addStyle(layoutCss);

    // Util functions
    window.DoubanUtils = {
        getParameterByName : function (name, url) {
            if (!url) url = window.location.href;
            name = name.replace(/[\[\]]/g, "\\$&");
            var regex = new RegExp("[?&]" + name + "(=([^&#]*)|&|#|$)"),
                results = regex.exec(url);
            if (!results) return null;
            if (!results[2]) return '';
            return decodeURIComponent(results[2].replace(/\+/g, " "));
        }
    };

    DoubanUtils.debugMode = DoubanUtils.getParameterByName('debug');
    console.log('Debug Mode: ' + DoubanUtils.debugMode);

    // Inject HTML

    $('#db-global-nav').before('<div id="debug-nav"><div>');

    var reactJsx = GM_getResourceText("reactJsx");
    var compiledReactJs = Babel.transform(reactJsx, { presets: ['react','es2015'] });
    eval(compiledReactJs.code);

})();