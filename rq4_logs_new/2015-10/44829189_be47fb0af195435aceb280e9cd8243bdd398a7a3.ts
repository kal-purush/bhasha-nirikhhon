/* Copyright 2011-2015 Jason Nelson (@iamcarbon)
   Free to use and modify under the MIT licence
   You must not remove this notice.
*/

module Carbon {
  export class Cropper {
    element  : any;
    viewport : Viewport;
    content  : ViewportContent;
    zoomer   : Slider;

    active   = false;
    dragging = false;

    mouseOffset: Point;
    startOffset: any;

    options: any;

    constructor(element: HTMLElement, options) {
      this.element = $(element);

      if (this.element.length == 0) throw new Error('element not found');

      this.viewport = new Viewport(this.element.find('.viewport')[0]);
      this.content  = new ViewportContent(this.element.find('.content'), this.viewport);

      this.viewport.content = this.content;

      this.options = options || { };
      this.mouseOffset = new Point(0, 0);

      this.viewport.element.addEventListener('mousedown', this.startDrag.bind(this), true);

      this.element.find('.content').css('cursor', 'grab');

      this.zoomer = new Slider(this.element.find('.zoomer')[0], {
        change : this.setScale.bind(this),
        end    : this.onSlideStop.bind(this)
      });

      if (this.content.calculateMinScale() > 1) {
        // We're streching. Disable zoom

        this.zoomer.hide();
      }

      let data = this.element.data();

      this.setScale(this.options.scale || 0);
      this.center();

      if (data.transform) {
        this.setTransform(data.transform);
      }

      this.element.data('controller', this);
    }

    onSlideStop() {
      this.element.triggerHandler({
        type      : 'change',
        transform : this.getTransform().toString()
      });
    }

    on(name: string, callback: Function) {
      $(this.element).on(name, callback);
    }

    off(name: string) {
      $(this.element).off(name);
    }

    startDrag(e: MouseEvent) {
      $(document).on({
        mousemove : this.moveDrag.bind(this),
        mouseup   : this.endDrag.bind(this)
      });

      this.element.addClass('dragging');

      // e.which == 1

      this.active = true;
      this.mouseOffset = new Point(e.clientX, e.clientY);

      this.startOffset = {
        top  : this.viewport.offset.top || 0,
        left : this.viewport.offset.left || 0
      };

      this.element.triggerHandler('start');

      e.preventDefault();
    }

    moveDrag(e) {
      if(!this.active) return;

      this.dragging = true;

      let distance = {
        x: e.clientX - this.mouseOffset.x,
        y: e.clientY - this.mouseOffset.y
      };

      let contentOffset = {
      	top  : Math.round(distance.y + this.startOffset.top),
        left : Math.round(distance.x + this.startOffset.left)
      };

      this.viewport.setOffset(contentOffset);
    }

    endDrag(e) {
      $(document).off('mousemove mouseup');

      this.element.removeClass('dragging');

      this.active = false;
      this.dragging = false;

      this.element.triggerHandler({
        type : 'change',
        transform : this.getTransform().toString()
      });
    }

    setImage(image: IMedia) {
      this.changeImage(image);
    }

    changeImage(image: IMedia) {
    	this.content.changeImage(image);

    	this.setScale(0);
    }

    center() {
    	this.viewport.centerContent();
    }

    setScale(value: number) {
      this.content.setRelativeScale(value);
      this.zoomer.setValue(value);
    }

    setTransform(text: string) {
      // 789x525/crop:273-191_240x140
      // rotate(90)/...

      var parts = text.split('/');

      var transformGroup = new TransformGroup();

      for (var i = 0, len = parts.length; i < len; i++) {
        var part = parts[i];

        if (part.indexOf(':') > -1) {
          // Flip crop origin from top left to bottom left

          var cropValue = part.split(':')[1];

          transformGroup.crop = {
            x      : parseInt(cropValue.split('_')[0].split('-')[0], 10),
            y      : parseInt(cropValue.split('_')[0].split('-')[1], 10),
            width  : parseInt(cropValue.split('_')[1].split('x')[0], 10),
            height : parseInt(cropValue.split('_')[1].split('x')[1], 10)
          };
        }
        else if (part.indexOf('x') > -1) {
          transformGroup.resize = {
            width  : parseInt(part.split('x')[0], 10),
            height : parseInt(part.split('x')[1], 10)
          };
        }
        else if (part.indexOf('rotate') > -1) {
          transformGroup.rotate = parseInt(part.replace('rotate(', '').replace(')', ''), 10);
        }
      }

      this.content.setSize(transformGroup.resize);

      var minWidth = this.content.calculateMinScale() * this.content.sourceWidth;
      var maxWidth = this.content.sourceWidth;

      var dif = maxWidth - minWidth;

      var relativeScale = (transformGroup.resize.width - minWidth) / dif;

      this.setScale(relativeScale);

      this.viewport.setOffset({ top: - transformGroup.crop.y, left: - transformGroup.crop.x });

      if (transformGroup.rotate) {
        this.content.rotate = transformGroup.rotate;
      }

      // stretch logic
      let stretched = this.viewport.content.calculateMinScale() > 1;

      this.element[stretched ? 'addClass' : 'removeClass']('stretched');

      return transformGroup;
    }

    getTransform() {
      let transformGroup = new TransformGroup();

      transformGroup.resize = {
        width: 	this.content.width,
        height: this.content.height
      };

      transformGroup.rotate = this.content.rotate;

      // Flip crop origin from top left to bottom left

      transformGroup.crop = {
        x      : (Math.abs(this.viewport.offset.left)) || 0,
        y      : (Math.abs(this.viewport.offset.top)) || 0,
        width  : this.viewport.width,
        height : this.viewport.height,
      };

      return transformGroup;
    }
  }

  class TransformGroup {
    rotate: number;
    resize: Size;
    crop: Rectangle;

    toString() {
      var parts = [];

      if (this.rotate) {
        parts.push('rotate(' + this.rotate + ')');
      }

      parts.push(this.resize.width + 'x' + this.resize.height);
      parts.push('crop:' + this.crop.x + '-' + this.crop.y + '_' + this.crop.width + 'x' + this.crop.height);

      return parts.join('/');
    }
  }

  export class Slider {
    element: HTMLElement;
    options: any;
    trackEl: HTMLElement;
    nubEl: HTMLElement;

    dragging = false;
    trackWidth: number;

    constructor(element: HTMLElement, options) {
      this.element = element;
      this.options = options || {};
      this.trackEl = <HTMLElement>this.element.querySelector('.track');
      this.nubEl = <HTMLElement>this.element.querySelector('.nub');

      this.trackEl.addEventListener('mousedown', this.startDrag.bind(this), true);
      this.trackEl.addEventListener('mouseup', this.endDrag.bind(this), true);

      this.nubEl.addEventListener('mousedown', this.startDrag.bind(this), true);
      this.nubEl.addEventListener('mouseup', this.endDrag.bind(this), true);

      this.trackWidth = this.trackEl.clientWidth;
    }

    hide() {
      this.element.style.display = 'none';
    }

    startDrag(e: MouseEvent) {
      e.preventDefault();

      this.dragging = true;
      this.moveTo(e);

      $(document).on({
      	mousemove : this.moveTo.bind(this),
      	mouseup   : this.endDrag.bind(this)
      });

      if (this.options.start) this.options.start();

      $(this.element).triggerHandler('start');
    }

    endDrag(e) {
      this.moveTo(e);
      this.dragging = false;

      $(document).off('mousemove mouseup');

      if (this.options.end) this.options.end();
    }

    setValue(value: number) {
      var nubWidth = this.nubEl.clientWidth;

      var x = Math.floor((this.trackWidth - nubWidth) * value);

    	this.nubEl.style.left = x + 'px';
    }

    moveTo(e: MouseEvent) {
      let position = Util.getRelativePosition(e.pageX, this.trackEl);

      this.nubEl.style.left = (position * 100) + '%';

      if (this.options.change) this.options.change(position);
    }
  }

  class Viewport {
    element: HTMLElement;
    width: number;
    height: number;
    center: Point;
    offset: any;
    content : ViewportContent;

    constructor(element: HTMLElement) {
      this.element = element;
      this.height  = this.element.clientHeight;
      this.width   = this.element.clientWidth;

      this.offset = {
        left: 0,
        top: 0
      };

      this.center = new Point(0, 0);
    }

    setSize(width: number, height: number) {
      this.element.style.width = width + 'px';
      this.element.style.height = height + 'px';

      this.height = height;
      this.width = width;
    }

    setOffset(offset) {
      if (offset.left > 0) {
        offset.left = 0;
      }

      if (offset.top > 0) {
        offset.top = 0;
      }

      var distanceToRightEdge = this.content.width - this.width + offset.left;

      if (distanceToRightEdge < 0) {
        offset.left = -(this.content.width - this.width);
      }

      var distanceToBottomEdge = this.content.height - this.height + offset.top;

      if (distanceToBottomEdge < 0) {
        offset.top = -(this.content.height - this.height);
      }

      // Set the offsets
      this.offset.left = Math.round(offset.left);
      this.offset.top = Math.round(offset.top);

      this.element.scrollLeft = -this.offset.left;
      this.element.scrollTop  = -this.offset.top;

      var leftToCenter = (-this.offset.left) + (this.width / 2);
      var topToCenter = (-this.offset.top) + (this.height / 2);

      this.center.x = (leftToCenter / this.content.width);
      this.center.y = (topToCenter / this.content.height);
    }

    recenter() {
      var x = this.content.width * (this.center.x);
      var y = this.content.height * (this.center.y);

      var leftOffset = -(((x * 2) - this.width) / 2);
      var topOffset = -(((y * 2) - this.height) / 2);

      this.setOffset({ left: leftOffset, top: topOffset });
    }

    centerContent() {
      this.center = new Point(0.5, 0.5);

      this.recenter();
    }
  }

  class ViewportContent {
    element: any;
    viewport: Viewport;
    sourceWidth: number;
    sourceHeight: number;

    scale = 1;

    width: number;
    height: number;

    relativeScale: LinearScale;
    rotate: number;

    constructor(element, viewport: Viewport) {
      this.element = $(element);
      this.viewport = viewport;

      var data = this.element.data();

      this.sourceWidth = data.width;
      this.sourceHeight = data.height;

      this.width = this.sourceWidth;
      this.height = this.sourceHeight;

      this.relativeScale = new LinearScale([this.calculateMinScale(), 1]); // to the min & max sizes
    }

    changeImage(image : IMedia) {
      var el = this.element[0];

      el.src = '';

      el.width = this.sourceWidth = image.width;
      el.height = this.sourceHeight = image.height;

      el.src = image.url;

      this.rotate = image.rotate;

      this.relativeScale = new LinearScale([this.calculateMinScale(), 1]);
   	}

    getCurrentScale() : number {
      return this.width / this.sourceWidth;
    }

    // The minimum size for the content to fit entirely in the viewport
    // May be great than 1 (stretched)
    calculateMinScale() : number {
      var minScale;
      let percentW = this.viewport.width / this.sourceWidth;
      let percentH = this.viewport.height / this.sourceHeight;

      if (percentH < percentW) {
        minScale = percentW;
      }
      else {
        minScale = percentH;
      }

      return minScale;
    }

    // TEMP
    setSize(size: Size) {
      this.width = size.width;
      this.height = size.height;

      this.scale = this.getCurrentScale();

      this.element.css({
        width: this.width + 'px',
        height: this.height + 'px'
      });

      this.viewport.recenter();
    }

    setRelativeScale(value: number) {
      if (value > 1) return;

      this.scale = this.relativeScale.getValue(value); // Convert to absolute scale

      // Scaled width & height
      this.width = Math.round(this.scale * this.sourceWidth);
      this.height = Math.round(this.scale * this.sourceHeight);

      this.element.css({
        width: this.width + 'px',
        height: this.height + 'px'
      });

      this.viewport.recenter();
    }
  }

  class LinearScale {
    domain: Array<number>;
    range: Array<number>;

    constructor(domain: Array<number>) {
      this.domain = domain || [0, 1];
      this.range = [0, 1]; // Always 0-1
    }

    getValue(value: number) : number {
      let lower = this.domain[0];
      let upper = this.domain[1];

      var dif = upper - lower;

      return lower + (value * dif);
    }
  }

  interface IMedia {
    width  : number;
    height : number;
    rotate : number;
    url    : string;
  }

  interface Size {
    width: number;
    height: number;
  }

  interface Rectangle {
    x: number;
    y: number;
    width: number;
    height: number;
  }

  class Point {
    constructor(public x: number,
                public y: number) { }
  }

  var Util = {
    getRelativePosition: function(x: number, relativeElement: HTMLElement) {
      return Math.max(0, Math.min(1, (x - this.findPosX(relativeElement)) / relativeElement.offsetWidth));
    },

    findPosX: function(element) {
      var curLeft = element.offsetLeft;

      while ((element = element.offsetParent)) {
        curLeft += element.offsetLeft;
      }

      return curLeft;
    }
  };
}