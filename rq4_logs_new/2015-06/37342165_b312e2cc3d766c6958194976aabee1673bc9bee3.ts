
var selectedElement:Element = null;
var currentX = 0;
var currentY = 0;
var cx = 0;
var cy = 0;

function cloneToTop(oldEl){
  // already at top, don't go farther…
  if(oldEl.atTop==true) 
      return oldEl;

  // make a copy of this node
  var el = oldEl.cloneNode(true);

  el.setAttribute("class", oldEl.getAttribute("class"));

  // select all draggable elements, none of them are at top anymore
  var dragEls= oldEl.ownerDocument.documentElement.querySelectorAll('.draggable');
  for(var i=0; i<dragEls.length; i++){
    dragEls[i].atTop=null;
  }

  var parent = oldEl.parentNode;
  // remove the original node
  parent.removeChild(oldEl);
  // insert our new node at top (last element drawn is first visible in svg)
  parent.appendChild(el);
  // Tell the world that our new element is at Top
  el.atTop= true;
  return el;
}


function selectElement(evt) {
  selectedElement = cloneToTop(evt.target);
  currentX = evt.clientX;
  currentY = evt.clientY;

  cx = parseInt(selectedElement.getAttribute("cx"));
  cy = parseInt(selectedElement.getAttribute("cy"));

  selectedElement.setAttribute("onmousemove", "moveElement(evt)");
  selectedElement.setAttribute("onmouseout", "deselectElement(evt)");
  selectedElement.setAttribute("onmouseup", "deselectElement(evt)");
}
    
function moveElement(evt) {

  var dx = evt.clientX - currentX;
  var dy = evt.clientY - currentY;

  cx += dx/3.1;
  cy += dy/3.1;
  
  selectedElement.setAttribute("cx", cx.toString());
  selectedElement.setAttribute("cy", cy.toString());

  currentX = evt.clientX;
  currentY = evt.clientY;

}
    
function deselectElement(evt) {
  if(selectedElement != null) {
      selectedElement.removeAttribute("onmousemove");
      selectedElement.removeAttribute("onmouseout");
      selectedElement.removeAttribute("onmouseup");
      selectedElement = null;
  }
}
        