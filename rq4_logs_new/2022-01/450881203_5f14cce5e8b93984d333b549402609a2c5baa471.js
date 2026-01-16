function createFastElement(childNode) {
  const fastElementObj = {};

  fastElementObj.target = childNode;

  fastElementObj.set = function(attr, value){
    this.target.setAttribute(attr,value);
  };

  fastElementObj.text = function(value){
    this.target.textContent = value;
  }

  fastElementObj.removeAll = function(){
    this.target.innerHTML = "";
  }

  fastElementObj.append = function(){
      for(let i of arguments){
        this.target.appendChild(i.target);
      }
  }

  return fastElementObj;
};

function useElement (tagName = []) {
  return tagName.map(element => createFastElement(document.createElement(element)));
};

function useRoot(rootName = ""){
  return (rootName.length === 0)? [] : [createFastElement(document.getElementById(rootName))];
}

function useListener(type, node) {

  function action (callback, useCapture) {
    node.target.addEventListener(type, callback, useCapture);
  };

  function removeAction() {
    node.target.removeEventListener(type, node.target, true);
  };

  return [action, removeAction];
};

function useStyle () {
  let children = arguments;

  function applyStyle (style) {
    for (let i in style) {
      Object.assign(children[i].target.style, style[i]);
    }
  };

  return [applyStyle];
};

function createPseudo (styleText) {
  let headData = [...document.head.children].toString().split(",");

  let styleTagContain = headData.filter(
    (data) => data === "[object HTMLStyleElement]"
  ).length;

  if (styleTagContain) {
    let styleElement = document.getElementsByTagName("style")[0];
    let existStyle = styleElement.textContent;
    let newStyle = styleText;
    styleElement.innerHTML =
      `${existStyle} /*new style added*/ ${newStyle}`.replace(/\s/g, "");
  } else {
    let styleElement = document.createElement("style");
    styleElement.appendChild(document.createTextNode(styleText));
    document.head.appendChild(styleElement);
  }
};

function repeat (tagName = "",count){
  return tagName.concat(" ").repeat(count).split(/(\w+)\s/g).filter(text=>text === tagName);
}

function useLink (callback){
  const generate = callback();
  generate();
}