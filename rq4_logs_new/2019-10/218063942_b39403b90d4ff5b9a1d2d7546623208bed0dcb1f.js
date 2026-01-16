// ==UserScript==
// @name         我的测试脚本 http://www.ttlsa.com/docs/greasemonkey/#basic
// @namespace    http://tampermonkey.net/
// @version      0.1
// @description  tamptest
// @author       zhaodong.xzd
// @match        https://www.baidu.com/*
// @exclude      https://www.baidu.com/link*
// @grant        GM_openInTab
// @require      http://code.jquery.com/jquery-1.11.1.min.js
// @run-at document-body
// ==/UserScript==
var jq = $.noConflict(true);
(function() {
    'use strict';
    var googleUrl='https://www.google.com/search?q='
    var kw=jq('#kw');
    // Your code here...
    // alert('Hello, from Tampermonkey.');
    var btngoogle='<input type="submit" value="Google" id="su2" class=" bg s_btn" style="margin-left: 10px;float: right;">'
    jq('#su').after(btngoogle);
    jq('#su2').click(function(){
        GM_openInTab(googleUrl+kw.val(),false)
    })
    console.log(node)
})();