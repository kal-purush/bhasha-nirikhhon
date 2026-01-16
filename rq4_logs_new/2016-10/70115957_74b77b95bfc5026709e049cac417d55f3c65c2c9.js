var ccTools = {

  /**
   * 改变this的指向到参数ele
   * @param  {Element} ele     [description]
   * @param  {Function} handler [description]
   * @return {[type]}         [description]
   */
  bindToThis: function(ele, handler) {
    return function() {
      return handler.apply(ele, arguments);
    }
  },

  /**
   * 绑定事件监听
   * @param {Element} ele       [description]
   * @param {EventType} eventType [description]
   * @param {Function} handler   [description]
   */
  addHandler: function(ele, eventType, handler) {
    if(ele.addEventListener) {
      // DOM 2.0
      // W3C标准addEventListener监听的事件处理函数handler内部的this指针，指向所绑定事件的对象ele本身
      ele.addEventListener(eventType, handler, false); 
    } 
    else if(ele.attachEvent) {
      // IE5+(IE8以下)attachEvent监听的事件处理函数handler内部的this指针，默认指向window对象
      // 可通过函数apply或call改变this指向所绑定事件的对象ele本身
      var h = ccTools.bindToThis(ele, handler);
      ele.attachEvent("on" + eventType, h);
    }
    else {
      ele["on" + eventType] = handler; // DOM 0
    }
  },
  
  /**
   * 解绑事件监听
   * @param {Element} ele       [description]
   * @param {EventType} eventType [description]
   * @param {Function} handler   [description]
   */
  removeHandler: function(ele, eventType, handler) {
    if(ele.removeEventListener) {
      ele.removeEventListener(eventType, handler, false);
    }
    else if(ele.detachEvent) {
      ele.detachEvent("on" + eventType, handler); 
    }
    else {
      ele["on" + eventType] = null;
    }
  },

  /**
   * 节点包含判断
   * @param  {Element} parentEle [description]
   * @param  {Element} ele       [description]
   * @param  {Element} opt_container [description]
   * @return {Boolean}           [description]
   */
  contains: function(parentEle, ele, opt_container) {
    if(parentEle == ele) {
      return true;
    }
    if(parentEle.contains) { // IE <= 8
      return parentEle.contains(ele);
    }
    if(parentEle.compareDocumentPosition) { // IE > 8 and others
      return parentEle.compareDocumentPosition(ele) == 16;
    }
    if(!ele || !ele.nodeType || ele.nodeType != 1) {
      return false;
    }
    // var prEl = el.parentNode;
    // while(prEl && prEl != container) {
    //    if (prEl == parentEl)
    //        return true;
    //    prEl = prEl.parentNode;
    // }
    return false;
  },

  /**
   * 鼠标移进移出
   * @param  {EventType} eventType    [description]
   * @param  {Function} handler      [description]
   * @return {Function}              [description]
   */
  mouseEvent: function(eventType, handler) {
    return function(e) {
      // IE > 8 and others
      if(window.addEventListener) {
        if(!ccTools.contains(this, e.relatedTarget)) {
          handler();
        }
      }
      // IE <= 8
      else {
        if(eventType == "mouseover") {
          if(!ccTools.contains(this, e.fromElement)) {
            handler();
          }
        }
        else if(eventType == "mouseout") {
          if(!ccTools.contains(this, e.toElement)) {
            handler();
          }
        }
      }
    }
  },
  
  /**
   * class选择器
   * @param  {String Array} cls       [description]
   * @param  {Element} opt_parentEle [description]
   * @return {Array}           [description]
   */
  getElementsByClassName: function(cls, opt_parentEle) {
    if(document.getElementsByClassName) {
      return (opt_parentEle || document).getElementsByClassName(cls);
    }
    else {
      var children = (opt_parentEle || document).getElementsByTagName('*');
      var targetEles = new Array();
      var len = children.length;
      for(var i=0; i<len; i++) {
        var child = children[i];
        var classNames = child.className.split(/\s+/g);
        var lens = classNames.length;
        for(var j=0; j<lens; j++) {
          if(classNames[j] == cls) {
            targetEles[targetEles.length] = child;
          }
        }
      }
      return targetEles;
    }
  },
  
  /**
   * 是否包含class
   * @param  {Object}  ele [description]
   * @param  {String}  cls [description]
   * @return {Boolean}     [description]
   */
  hasClass: function(ele, cls) {
    var classNameArray = ele.className.split(/\s+/g);
    for(var i = 0; i < classNameArray.length; i++) {
      if(classNameArray[i] == cls) {
        return true;
      }
    }
    return false;
  },

  /**
   * 添加class
   * @param {Object} ele [description]
   * @param {String} cls [description]
   */
  addClass: function(ele, cls) {
    if(!this.hasClass(ele, cls)) {
      ele.className = (ele.className == '') ? cls : ele.className + ' ' + cls;
    }
  },

  /**
   * 删除class
   * @param {Object} ele [description]
   * @param {String} cls [description]
   */
  removeClass: function(ele, cls) {
    if(this.hasClass(ele, cls)) {
      var cls_obj = ' ' + ele.className + ' '; // 获取 class 内容, 并在首尾各加一个空格
      cls_obj = cls_obj.replace(/(\s+)/gi, ' '); // 将多余的空字符替换成一个空格
      var new_class = cls_obj.replace(' ' + cls + ' ', ' '); // 替换掉首尾加了空格的cls
      new_class = new_class.replace(/(^\s+)|(\s+$)/g, ''); // 去掉收尾空格
      ele.className = new_class;
    }
  },

  /**
   * 字符数字每三位添加符号分隔
   * @param  {String} strNum [description]
   * @param  {String} symbol [description]
   * @return {String}        [description]
   */
  stringNumberAddSymbol: function(strNum, symbol) {
    var len = new String(strNum).length;
    if(len < 3) {
      return strNum;
    }
    else {
      var left = len % 3;
      var extraStrNum = strNum.match(eval('/\\d{' + left + '\}/'));
      var targetStrNum = strNum.replace(eval('/\\d{' + left + '\}/'), '');
      var targetStrNumLists = targetStrNum.match(/\d{3}/g);
      var newStrNum = '';
      for(var i = 0; i < targetStrNumLists.length; i++) {
        newStrNum += targetStrNumLists[i].replace(/\d{3}/, symbol + targetStrNumLists[i]);
      }

      if(left == 0) {
        return newStrNum.replace(eval('/\\' + symbol + '+?/'), '');
      }
      else {
        return newStrNum.replace(/.*/, extraStrNum[0] + newStrNum);
      }
    } 
  },

  /**
   * 字符数字每三位删除符号分隔
   * @param  {String} strNum [description]
   * @param  {String} symbol [description]
   * @return {String}        [description]
   */
  stringNumberRemoveSymbol: function(strNum, symbol) {
    return strNum.replace(eval('/\\' + symbol + '*/g'), '');
  },

  /**
   * window.onload 网页加载完成时执行特定的方法
   * @param {Function} func
   */
  addLoadEvent: function(func) {
    var oldonload = window.onload;
    if(typeof window.onload != "function") {
      window.onload = func;
    } else {
      window.onload = function() {
        oldonload();
        func();
      }
    }
  },

  /**
   * 在目标节点之后插入新节点
   * @param  {Object} newEle
   * @param  {Object} targetEle
   */
  insertAfter: function(newEle, targetEle) {
    var parentEle = targetEle.parentNode;
    if(parentEle.lastChild == targetEle) {
      parentEle.appendChild(newEle);
    } else {
      parentEle.insertBefore(newELe, targetEle.nextSibling);
    }
  }

}