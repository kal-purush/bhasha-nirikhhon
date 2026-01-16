
(function ($) {
    'use strict';    

    var FileUpload = function (el, opt) {
        var def = {
            classes:{
                fileUpload:'file-browse__upload',
                fileInput:'file-browse__input'
            }           
        }, 
        _self = this;
        _self.opts = $.extend({}, def, opt), _self.$el="";
  
        function _init() {  
           _self.$el = $(el), 
           _self.$fileUpload = _self.$el.find("."+_self.opts.classes.fileUpload),
           _self.$fileInput = _self.$el.find("."+_self.opts.classes.fileInput); 
           
           _initEvents();
        }          

        function _initEvents() {
            _self.$fileUpload.on('change.fileUpload',function () {
                _self.$fileInput.val(_self.$fileUpload.val());
            });
        }     

        _init()
    }    
    
    function Plugin(option) {
        return this.each(function () {
            var $this = $(this),
                curEl = $this.data('fileUpload'),
                options = typeof option == 'object' && option;

            if (curEl) {
               
            }
            if (!curEl) $this.data('fileUpload', (curEl = new FileUpload(this, options)))
        })
    }

    var old = $.fn.fileUpload

    $.fn.fileUpload = Plugin
    $.fn.fileUpload.Constructor = FileUpload
  

    $.fn.fileUpload.noConflict = function () {
        $.fn.fileUpload = old
        return this
    }

}(jQuery));