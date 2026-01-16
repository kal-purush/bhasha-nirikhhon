// ==UserScript==
// @name        Google Navigator Using Tab
// @namespace   Violentmonkey Scripts
// @match       https://www.google.com/search
// @grant       none
// @version     1.0
// @author      -
// @run-at      document-end
// @description 13/09/2022, 22:09:16
// ==/UserScript==
let count = 0;
let searchResultsSelector = '.MjjYud'
const results = Array.from(document.querySelectorAll(searchResultsSelector));

results.forEach(result=>{
  try{
      result.querySelectorAll('a').forEach(link=>{
      link.tabIndex = ++count;
    })
  }catch(error){}  
})


document.onkeydown = (e)=>{
  if(e.key === ' '){
    if(document.activeElement.tagName === 'A'){
      e.preventDefault();
      document.activeElement.click();
    }
  }
}