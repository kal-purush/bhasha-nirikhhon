// All rights reserved Georgios Kormaris 2014
"use strict";
//TODO: Attach styled ::after, horizontal
var prePar = [];
var allYarns = [];
chrome.extension.sendMessage({}, function (response) {
  var readyStateCheckInterval = setInterval(function () {
    if (document.readyState === "complete") {
      clearInterval(readyStateCheckInterval);

      // Page loaded

      var hoverLoop = function (e) {
        var curPar = [];
        var sameIndex = 0;
        var curElem = getParents(this, 'entry'); //The parList contains the .entry divs USE >THIS< to get the element// that fired the event, as e.target returns the hovered element which could be a child
        curElem.reverse();
        for (var ind = 0; ind < curElem.length; ind++) {
          //curElem[ind] = curElem[ind].children[1]; //We need the second child: the actual text
          var temp = new Comment(curElem[ind].getElementsByTagName('form')[0]);
          curPar.push(temp);
        }
        sameIndex = getSameIndex(curPar, prePar);
        //var delY = new CommentList(prePar.slice(sameIndex, prePar.length)); //For removal
        //var newY = new CommentList(curPar.slice(sameIndex, curPar.length));
        var newY = new CommentList(curPar.slice());
        for (var j = 0; j < allYarns.length; j++) {
          allYarns[j].parentNode.removeChild(allYarns[j]);
        }
        allYarns = [];
        newY.createYarn();
        //newY.removeYarn();
        //delY.removeYarn();
      };


      var entryList = document.getElementsByClassName('entry');
      for (var i = 0; i < entryList.length; i++) {  //The main loop
        entryList[i].addEventListener("mouseover", hoverLoop);
      }
    }
  }, 10);
});


function getSameIndex(cur, pre) {
  var curList = cur;
  var preList = pre;
  //var drawIndex = 0;
  var end = false;
  var k = 0;

  if (preList[0] !== undefined) {
    if (curList.length >= preList.length) { //We don't want an out-of-range error.
      while (end === false && k < preList.length) {
        if (curList[k].getId() !== preList[k].getId()) {
          //drawIndex = k;
          end = true;
        } else {
          k++;
        }
      }
    } else {
      while (end === false && k < curList.length) {
        if (curList[k].getId() !== preList[k].getId()) {
          //drawIndex = k;
          end = true;
        } else {
          k++;
        }
      }
    }
  }
  return k - 1;
}

/******** COMMENTLIST-- ********/
function CommentList(comList) {
  this._comments = comList;
}

CommentList.prototype.createYarn = function () {
  var angleDeg, widthPx = 0;
  var p1, p2;
  for (var i = 1; i < this._comments.length; i++) { //Start from the second comment
    p1 = this._comments[i - 1].getCorner(); //The previous element
    p2 = this._comments[i].getCorner();
    angleDeg = 180 + (Math.atan2(p2.getY() - p1.getY(), p2.getX() - p1.getX()) * 180 / Math.PI);
    widthPx = Math.sqrt((p2.getX() - p1.getX()) * (p2.getX() - p1.getX()) + (p2.getY() - p1.getY()) * (p2.getY() - p1.getY())); //Marginally better than obfuscated machine code
    var tempYarn = new Yarn(widthPx, angleDeg, this._comments[i].getCorner());
    this._comments[i].addYarn(tempYarn);
  }
};

CommentList.prototype.removeYarn = function () {
  if (this._comments.length > 0) {
    for (var i = 0; i < this._comments.length; i++) {
      this._comments[i].removeY();
    }
  }
};
/******** --COMMENTLIST ********/


/******** POINT-- ********/
function Point(xVal, yVal) {
  this._x = xVal;
  this._y = yVal;
}

Point.prototype.getX = function () {
  return this._x;
};

Point.prototype.getY = function () {
  return this._y;
};

/******** --POINT ********/

/*
 function Yarn(idVal, pointObj) {
 this._yid = idVal;
 this._corner = pointObj;

 this.drawYarn = function (i, currentL, previousL) {
 for (var j = i; j < currentL.length - 1; j++) { //Start drawing only where needed
 var pointHi = currentL[j].corner();
 var pointLo = currentL[j + 1].corner();
 var angle = calcAngle(pointHi, pointLo);
 var width = calcWidth(pointHi, pointLo);
 addYarn(angle, width);
 }
 }

 var addYarn = function (angl, widt) {
 var yElement = document.createElement('div');
 yElement.id = yid;
 yElement.style.top = corner.y.toString();
 yElement.style.left = corner.x.toString();
 yElement.style.width = widt.toString();
 yElement.style.transform = "rotateZ(" + angl + ")";
 }

 }
 */

/******** YARN-- ********/
function Yarn(widthVal, angleVal, pointObj) { //TODO: extend DivElement
  this._width = widthVal;
  this._angle = angleVal;
  this._origin = pointObj;
  this._divElem = document.createElement("div");
}

Yarn.prototype.addToDoc = function () {
  document.body.appendChild(this._divElem);
  this._divElem.classList.add('yarnstring');
  this._divElem.style.width = this._width.toString() + "px";
  this._divElem.style.transform = "rotateZ(" + this._angle.toString() + "deg)";
  this._divElem.style.top = this._origin.getY().toString() + "px";
  this._divElem.style.left = this._origin.getX().toString() + "px";
  allYarns.push(this._divElem);
};

Yarn.prototype.removeFromDoc = function () {
  document.body.removeChild(this._divElem);
};

/******** --YARN ********/

//KEEP DATA INTACT, CHANGE THE FUNCTION IN CHILD>
/******** COMMENT-- ********/
function Comment(elemObj) {
  this._elem = elemObj;
  this._corner = new Point(getCumulativeOffset(elemObj).x, getCumulativeOffset(elemObj).y);
  this._yarn = new Yarn(null, null, null);
  this._elemId = elemObj.id;
}

Comment.prototype.getCorner = function () {
  return this._corner;
};

Comment.prototype.getId = function () {
  return this._elemId;
};

Comment.prototype.addYarn = function (yarnObj) {
  this._yarn = yarnObj;
  this._yarn.addToDoc();
};

Comment.prototype.removeY = function () {
  this._yarn.removeFromDoc();
  this._yarn = undefined;
};
/******** --COMMENT ********/

/******** 1stCOMMENT-- ********/
function FirstComment(elemObj) {
  Comment.call(this, elemObj);
}

FirstComment.prototype = Object.create(Comment.prototype);
FirstComment.prototype.constructor = FirstComment;
/******** --1stCOMMENT ********/

/*
 function ParList(elmn) {
 this.parList = getParents(elmn, '.entry'); //The parList contains the .entry divs
 for (var ind = 0; ind < this.parList.length; ind++) {
 this.parList[ind] = this.parList[ind].children[1]; //We need the second child: the actual text
 }
 //The first element is always the one hovered.
 }
 */


function getParents(el, classNam) {
  var parents = [el];

  var p = el.parentNode;
  while (p !== document) {
    var o = p;
    if (hasClass(o.previousElementSibling, classNam)) {
      parents.push(o.previousElementSibling);
      p = o.previousElementSibling;
    } else {
      p = o.parentNode;
    }
  }
  return parents; // returns an Array []
}

function getCumulativeOffset(obj) {
  var left, top;
  left = top = 0;
  if (obj) { //truthy
    if (obj.offsetParent) {
      do {
        left += obj.offsetLeft;
        top += obj.offsetTop;
      } while (obj === obj.offsetParent);
    }
  }
  return {
    x: left,
    y: top
  };
}

function hasClass(element, cls) {
  if (element !== null) {
    return (' ' + element.className + ' ').indexOf(' ' + cls + ' ') > -1;
  } else {
    return false;
  }
}


/** ***************************************************** **/
/** removeChild is silly, let's create remove()
 * I could use remove() out of the box since this is for Chrome...
 * so that's what I'm going to do
 */
Element.prototype.remove = function () {
  this.parentElement.removeChild(this);
};

NodeList.prototype.remove = HTMLCollection.prototype.remove = function () {
  for (var i = 0; i < this.length; i++) {
    if (this[i] && this[i].parentElement) {
      this[i].parentElement.removeChild(this[i]);
    }
  }
};
 /*
 * It even works in Firefox if I decide to re-use the code so this isn't needed at all
 * I think */