"use strict";

var cookieUtil = {
  getCookie: function getCookie(name) {
    if (typeof window !== 'undefined') {
      var splitArray = document.cookie.split(name + "=");
      if (splitArray.length === 2) {
        return splitArray[1];
      }
    }
    return null;
  },
  setCookie: function setCookie(name, value, expires) {
    var now = new Date();
    var expireDate = new Date(now.getTime() + expires);
    document.cookie = "name=" + escape(value) + (expires ? ";expires=" + expireDate.toGMTString() : null);
  }
};

module.exports = cookieUtilt;