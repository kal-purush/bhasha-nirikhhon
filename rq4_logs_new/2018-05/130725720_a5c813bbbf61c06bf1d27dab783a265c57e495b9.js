/******/ (function(modules) { // webpackBootstrap
/******/ 	// The module cache
/******/ 	var installedModules = {};
/******/
/******/ 	// The require function
/******/ 	function __webpack_require__(moduleId) {
/******/
/******/ 		// Check if module is in cache
/******/ 		if(installedModules[moduleId]) {
/******/ 			return installedModules[moduleId].exports;
/******/ 		}
/******/ 		// Create a new module (and put it into the cache)
/******/ 		var module = installedModules[moduleId] = {
/******/ 			i: moduleId,
/******/ 			l: false,
/******/ 			exports: {}
/******/ 		};
/******/
/******/ 		// Execute the module function
/******/ 		modules[moduleId].call(module.exports, module, module.exports, __webpack_require__);
/******/
/******/ 		// Flag the module as loaded
/******/ 		module.l = true;
/******/
/******/ 		// Return the exports of the module
/******/ 		return module.exports;
/******/ 	}
/******/
/******/
/******/ 	// expose the modules object (__webpack_modules__)
/******/ 	__webpack_require__.m = modules;
/******/
/******/ 	// expose the module cache
/******/ 	__webpack_require__.c = installedModules;
/******/
/******/ 	// define getter function for harmony exports
/******/ 	__webpack_require__.d = function(exports, name, getter) {
/******/ 		if(!__webpack_require__.o(exports, name)) {
/******/ 			Object.defineProperty(exports, name, {
/******/ 				configurable: false,
/******/ 				enumerable: true,
/******/ 				get: getter
/******/ 			});
/******/ 		}
/******/ 	};
/******/
/******/ 	// getDefaultExport function for compatibility with non-harmony modules
/******/ 	__webpack_require__.n = function(module) {
/******/ 		var getter = module && module.__esModule ?
/******/ 			function getDefault() { return module['default']; } :
/******/ 			function getModuleExports() { return module; };
/******/ 		__webpack_require__.d(getter, 'a', getter);
/******/ 		return getter;
/******/ 	};
/******/
/******/ 	// Object.prototype.hasOwnProperty.call
/******/ 	__webpack_require__.o = function(object, property) { return Object.prototype.hasOwnProperty.call(object, property); };
/******/
/******/ 	// __webpack_public_path__
/******/ 	__webpack_require__.p = "";
/******/
/******/ 	// Load entry module and return exports
/******/ 	return __webpack_require__(__webpack_require__.s = 0);
/******/ })
/************************************************************************/
/******/ ([
/* 0 */
/***/ (function(module, exports, __webpack_require__) {

const DOMNodeCollection = __webpack_require__(1);
const main_functions = __webpack_require__(2);
const GameView = __webpack_require__(7);


  $l(function() {
   let board = new GameView();
  })


/***/ }),
/* 1 */
/***/ (function(module, exports) {

class DOMNodeCollection {
  constructor(elements){ //should only receive an array
    this.elements = elements
  }


  html(arg) {
    if (!arg) {
      return this.elements[0].textContent;
    } else {
      this.elements.forEach(node => {
        node.innerHTML = arg
      });
    }
  }

  empty() {
    this.elements.forEach(node => {
      node.innerHTML = "";
    });
  }

  append(arg) {
    this.elements.forEach(node => {
      node.innerHTML = node.innerHTML + arg;
    });
  }


  children() {
    let childrenArray = [];
    this.elements.forEach(node => {
      for (let i = 0; i < node.children.length; i++) {
        childrenArray.push(node.children[i])
      }
    });
    return new DOMNodeCollection(childrenArray);
  }

  parent() {
    let parentsArray = [];

    this.elements.forEach(node => {
      parentsArray.push(node.parentNode);
    });
    return new DOMNodeCollection(parentsArray);
  }

  find(arg) {
    let matchingDescendants = [];

    this.elements.forEach(node => {
      matchingDescendants.push(node.querySelectorAll(arg));
    });

    let matchingDescendantsArray = [];

    for(let i=0; i < matchingDescendants.length; i++) {
      for (let j=0; j < matchingDescendants[i].length; j++) {
        matchingDescendantsArray.push(matchingDescendants[i][j]);
      }
    }
    return new DOMNodeCollection(matchingDescendantsArray);
  }

  remove(arg) {

   this.elements.forEach(node => {
    if (!arg){
      node.remove();
      } else {
        arg === "." + node.className ? node.remove() : null
      }
   });
  }

  on(e, callback) {
    this.elements.forEach(node => {
      node.addEventListener(e, callback)
      node.callback = callback;
    });
  }

  off(e) {
    this.elements.forEach(node => {
      node.removeEventListener(e, node.callback);
    });
  }

  attr(attribute, value) {

      if (!value) {
        if (this.elements[0].attributes[attribute] === undefined) {
          return undefined
        } else {
        return this.elements[0].attributes[attribute].nodeValue
        }

      } else {
        this.elements.forEach((element) => {
          if (element.attributes[attribute]) {
            element.attributes[attribute].nodeValue= value;
          } else {
            element.setAttribute(attribute, value);
          }
        });
      }
  }

  addClass(classNames) {
    this.elements.forEach(element => {
      if (element.classList.length === 0) {
      element.setAttribute("class", classNames)
      } else {
        let classNamesSplit = classNames.split(" ");
        for(let i = 0; i < classNamesSplit.length; i++) {
          element.classList.add(classNamesSplit[i]);
        }

      }
    });
  }
  //
  removeClass(classNames) {
    if (!classNames) {
      this.elements.forEach(element => {
        element.setAttribute("class", "");
      });
    } else {
      let classNamesSplit = classNames.split(" ");
      this.elements.forEach(element => {
        let classListLength = element.classList.length;

        for(let i = 0; i < classListLength; i++) {
          for (let j = 0; j < classNamesSplit.length; j++) {
            if (element.classList[i] === classNamesSplit[j]) {
              element.classList.remove(classNamesSplit[j]);
            }
          }
        }
      });
    }
  }

}

module.exports = DOMNodeCollection;


/***/ }),
/* 2 */
/***/ (function(module, exports) {

throw new Error("Module build failed: Error: ENOENT: no such file or directory, open '/Users/miqueas/Desktop/jQueryLight/jquerylib/main_functions.js'\n    at Error (native)");

/***/ }),
/* 3 */,
/* 4 */,
/* 5 */,
/* 6 */,
/* 7 */
/***/ (function(module, exports) {

throw new Error("Module build failed: Error: ENOENT: no such file or directory, open '/Users/miqueas/Desktop/jQueryLight/js/game_view.js'\n    at Error (native)");

/***/ })
/******/ ]);