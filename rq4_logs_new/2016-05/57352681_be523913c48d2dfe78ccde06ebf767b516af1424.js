'use strict';

var s = document.createElement('script');
s.src = chrome.extension.getURL('js/insertScript.js');
var l = document.createElement('script');
l.src = chrome.extension.getURL('js/listeners.js');

(document.head||document.documentElement).appendChild(s);
(document.head||document.documentElement).appendChild(l);
// s.onload = function() {
//   s.parentNode.removeChild(s);
// };

// l.onload = function () {
//   l.parentNode.removeChild(l);
// }