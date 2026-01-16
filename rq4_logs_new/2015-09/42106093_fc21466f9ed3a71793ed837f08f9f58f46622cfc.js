(function() {
  'use strict';
  //Define the base Component Object
  var MaterialComponent = function MaterialComponent(element) {
    this.element_ = element; // keep a reference the component element
    this.init(); // initialize the component
  };
  window.MaterialComponent = MaterialComponent;
  // Group together Constant data
  MaterialComponent.prototype.Constant_ = {/* None for now.*/};
  // Store the class names the component utilizes in one place (for convenience)
  MaterialComponent.prototype.CssClasses_ = {
    CONTAINER: 'mdl-component__container',
    IS_ACTIVE: 'is-active',
    IS_VISIBLE: 'is-visible'
  };
  // Setup event handling
  MaterialComponent.prototype.onChange_ = function(event) {
    this.updateClasses_();
  };
  MaterialComponent.prototype.onFocus_ = function(event) {
    this.element_.classList.add(this.CssClasses_.IS_FOCUSED);
  };
  MaterialComponent.prototype.blurHandler_ = function(event) {
    if (event) this.element_.blur();
  };
  MaterialComponent.prototype.onMouseUp_ = function(event) {
    this.blur_();
  };
  MaterialComponent.prototype.onMouseDown_ = function(event) {
    this.blur_();
  };
  MaterialComponent.prototype.handleMouseEnter_ = function(event) {
    event.stopPropagation();
    var props = event.target.getBoundingClientRect();
    var left = props.left + (props.width / 2);
    var marginLeft = -1 * (this.element_.offsetWidth / 2);
    if (left + marginLeft < 0) {
      this.element_.style.left = 0;
      this.element_.style.marginLeft = 0;
    } else {
      this.element_.style.left = left + 'px';
      this.element_.style.marginLeft = marginLeft + 'px';
    }
    this.element_.style.top = props.top + props.height + 10 + 'px';
    this.element_.classList.add(this.CssClasses_.IS_ACTIVE);
    window.addEventListener('scroll', this.boundMouseLeaveHandler, false);
    window.addEventListener('touchmove', this.boundMouseLeaveHandler, false);
  };
  MaterialComponent.prototype.handleMouseLeave_ = function(event) {
    event.stopPropagation();
    this.element_.classList.remove(this.CssClasses_.IS_ACTIVE);
    window.removeEventListener('scroll', this.boundMouseLeaveHandler);
    window.removeEventListener('touchmove', this.boundMouseLeaveHandler, false);
  };
  MaterialComponent.prototype.handleClick_ = function(event) {
    'use strict';
    this.movable_.classList.remove(this.CssClasses_.POSITION_PREFIX + this.position_);
    this.movable_.classList.remove(this.Constant_.ANIMATIONS[this.position_]);
    this.position_++;
    if (this.position_ > 5) {
      this.position_ = 0;
    }
    this.movable_.classList.add(this.Constant_.ANIMATIONS[this.position_]);
    this.movable_.classList.add(this.CssClasses_.POSITION_PREFIX + this.position_);
  };
  // Public Methods
  MaterialComponent.prototype.fooBar = function() {
    //do something
  };
  // Define the components' initialization process
  MaterialComponent.prototype.init = function() {
    if (this.element_) {
      var forElId = this.element_.getAttribute('for');
      if (forElId) this.forElement_ = document.getElementById(forElId);
      if (this.forElement_) {
        if (!this.forElement_.getAttribute('tabindex')) this.forElement_.setAttribute('tabindex', '0');
        this.boundMouseEnterHandler = this.handleMouseEnter_.bind(this);
        this.boundMouseLeaveHandler = this.handleMouseLeave_.bind(this);
        this.forElement_.addEventListener('mouseenter', this.boundMouseEnterHandler,false);
        this.forElement_.addEventListener('click', this.boundMouseEnterHandler,false);
        this.forElement_.addEventListener('blur', this.boundMouseLeaveHandler);
        this.forElement_.addEventListener('touchstart', this.boundMouseEnterHandler, false);
        this.forElement_.addEventListener('mouseleave', this.boundMouseLeaveHandler);
      }
    }
  };
  componentHandler.register({
    constructor: MaterialComponent,
    classAsString: 'MaterialComponent',
    cssClass: 'demo-js-animation'
  });
})();