/**
 * jQuery responsiveShopifyImg
 * Simple jQuery plugin for responsive image support within Shopify stores using Shopify's provided image sizes.
 * http://mrpunkin.github.com/jquery.responsiveShopifyImg.js
 *
 * Licensed under the MIT license.
 * Copyright 2015 Bryan Corey
 * https://github.com/mrpunkin
 */

;(function($, window, document, undefined){
  var pluginName = "responsiveShopifyImg";

  $.fn[pluginName] = function(){
    $.data(window, pluginName, {
      elements : $(this),
      sizes : {
        pico : 16,
        icon : 32,
        thumb : 50,
        small : 100,
        compact : 160,
        medium : 240,
        large : 480,
        grande : 600,
        '1024x1024': 1024,
        '2048x2048': 2048,
        master : 2048
      }
    });

    var resize = function(e){
      var p = $.data(window, pluginName),
          size;

      var sizes = []
      $.each(p.sizes, function(k,v){ sizes.push([k, v]) });
      sizes.sort(function(a,b){ return a[1] - b[1] });
      console.log(sizes);

      p.elements.each(function(){
        var el = this,
            s = $(el).width();

        if($(el).height() > s) s = $(el).height();
        if(window.devicePixelRatio) s = s * window.devicePixelRatio;

        $.each(sizes, function(i, a){
          if(s - a[1] < 0){
            size = a[0];
            return false;
          }
        });

        var sizeOpts = $.map(p.sizes, function(v,s) { return s; }).join("|"),
            regex = new RegExp("_("+sizeOpts+")\."),
            src = $(el).data('shopify-src').replace(regex, ".").replace(/\.(jpg|png|gif)/, "_"+size+".$1"),
            loader = new Image();

        loader.onload = function(){
          if(el.tagName.toLowerCase() == "img"){
            el.src = src;
          }else{
            $(el).css('background-image', 'url("'+src+'")');
          }
        };
        loader.src = src;
      });
    };

    $(window).on("resize."+pluginName+" orientationchange."+pluginName, resize).trigger('resize.'+pluginName);

    return this;
  };
}(jQuery, window, document));