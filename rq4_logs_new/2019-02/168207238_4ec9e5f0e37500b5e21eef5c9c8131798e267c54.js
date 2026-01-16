import $ from 'jquery';
require('webpack-jquery-ui');

$.widget('jtrello.card', {
    options: {},
  
    _create: function() {},

    _destroy: function() {
        this.element.remove();
    }
});