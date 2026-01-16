Math.easeInOutQuad = function(t, b, c, d) {
    t /= d / 2;
    if (t < 1) return c / 2 * t * t + b;
    t--;
    return -c / 2 * (t * (t - 2) - 1) + b;
  };
  export function isScrolledIntoView(elem, offsetVal = 0) {
    var docViewTop = window.pageYOffset;
    var docViewBottom = docViewTop + window.innerHeight;
    var elemTop = offset(elem).top;
    var elemBottom = elemTop + elem.clientHeight;
    return docViewTop >= elemTop - (offsetVal) /*- window.innerHeight*/ ; // /((elemBottom <= docViewBottom) && (elemTop >= docViewTop));
  }
  
  export function offset(el) {
    var rect = el.getBoundingClientRect(),
      scrollLeft = window.pageXOffset || document.documentElement.scrollLeft,
      scrollTop = window.pageYOffset || document.documentElement.scrollTop;
    return {
      top: rect.top + scrollTop,
      left: rect.left + scrollLeft
    }
  }
  
  
  
  export function isInViewport(el, offset) {
    var top = el.offsetTop + offset;
    var left = el.offsetLeft;
    var width = el.offsetWidth;
    var height = el.offsetHeight;
  
    while (el.offsetParent) {
      el = el.offsetParent;
      top += el.offsetTop;
      left += el.offsetLeft;
    }
  
    return (
      top < (window.pageYOffset + window.innerHeight) &&
      left < (window.pageXOffset + window.innerWidth) &&
      (top + height) > window.pageYOffset &&
      (left + width) > window.pageXOffset
    );
  };
  
  export class scrollToAnimate {
    constructor(element, to, duration) {
      this.increment = 20;
      this.setTimeoutInst = null;
    }
  
    animate(element, to, duration) {
      clearTimeout(this.setTimeoutInst);
      this.currentTime = 0;
      this.element = element;
      this.duration = duration;
      this.start = element.scrollTop;
      this.change = to - this.start;
      this.animateScroll();
    }
  
    clear(){
      this.currentTime = 0;
      clearTimeout(this.setTimeoutInst);
    }
  
    animateScroll() {
      this.currentTime += this.increment;
      var val = Math.easeInOutQuad(this.currentTime, this.start, this.change, this.duration);
      this.element.scrollTop = val;
      if (this.currentTime < this.duration) {
        this.setTimeoutInst = setTimeout(this.animateScroll.bind(this), this.increment);
      } else {
        this.currentTime = 0;
      }
    };
  }