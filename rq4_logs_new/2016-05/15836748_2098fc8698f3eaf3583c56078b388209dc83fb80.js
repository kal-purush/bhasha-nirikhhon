'use strict';

var el = ['fb', 'twt', 'git', 'mail'];
var wait = 200;

el.forEach(function(item, ind) {
  setTimeout(function() {
    document.getElementById('social').children[ind].style.right = '0';
  }, wait);
  wait += 100;
})