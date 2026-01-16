document.getElementsByTagName('select')[0].onchange = function() {
  var index = this.selectedIndex;
  var inputText = this.children[index].innerHTML.trim();
  var iframe = document.getElementsByTagName('iframe')[0];
  if (inputText !== "-") {
    iframe.src = inputText + ".html";
    iframe.style = "display:block;";
  } else {
    iframe.style = "display:none;";
  }
  
  document.getElementsByTagName('a')[0].text = "Open in new window";
  document.getElementsByTagName('a')[0].href = inputText + ".html";

};