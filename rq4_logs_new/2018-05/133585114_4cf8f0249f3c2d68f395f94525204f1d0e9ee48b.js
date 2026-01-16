// ==UserScript==
// @name         Youtube Downloader
// @namespace    TheWebs
// @version      0.1
// @description  Download youtube videos as .mp3
// @author       Francisco Leal
// @match        https://www.youtube.com/watch?v=*
// @grant        none
// ==/UserScript==

(function() {
    'use strict';
    var id;
    function teste(){
        try {
            var elemento = document.getElementsByTagName("yt-view-count-renderer")[0];
            var url = document.location.href;
            url = 'https://www.converto.io/?' + url.split('?')[1];
            elemento.insertAdjacentHTML("beforeBegin", "<a href='" + url + "' style='margin-bottom: 10px;border: 2px solid black; background-color:red;color:white;text-decoration:none;border-radius: 3px;'>Download</a>");
            clearInterval(id);
        }
        catch(err) {
            alert(err);
        }
    }
    setTimeout(function(){id = setInterval(teste(), 1000);}, 1500);
    
}


)();