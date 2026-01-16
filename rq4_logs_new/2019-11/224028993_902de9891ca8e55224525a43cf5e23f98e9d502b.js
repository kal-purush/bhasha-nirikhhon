 'use strict'

 var i = 0;

 var colors = ['red', 'blue', 'white', 'yellow', 'green', 'black', 'orange', 'gray', 'brown', 'pink'];

 var btnNext = document.querySelector('.btnNext');

 var btnPrev = document.querySelector('#btnPrev');

 //  btnNext.onclick = renderNext();

 function renderNext() {
     document.getElementById('kvadrat').style.cssText = `background-color: ${colors[i++ % colors.length]};`;
 };

 function renderPrev() {
     document.getElementById('kvadrat').style.cssText = `background-color: ${colors[i-- % colors.length]};`;
 };