// ==UserScript==
// @name         juejin
// @namespace    http://tampermonkey.net/
// @version      0.1
// @description  try to take over the world!
// @author       Aaron Lui
// @match        *://juejin.cn/*
// @icon         https://www.google.com/s2/favicons?sz=64&domain=juejin.cn
// @grant        none
// ==/UserScript==

(function() {
  'use strict';

  // Your code here...
  var isHide = localStorage.getItem('hideExtension')
  if (!isHide) {
    localStorage.setItem('hideExtension', true)
  }
})();