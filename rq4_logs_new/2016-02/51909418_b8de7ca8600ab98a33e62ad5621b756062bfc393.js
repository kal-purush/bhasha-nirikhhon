/*获取url参数*/
;(function($){
  $.extend({
    getUrlParam:function(name,url)
    {
        var reg = new RegExp("(^|&)"+ name +"=([^&]*)(&|$)"),
            r_txt = url?url:window.location.search;
            r = r_txt.substr(1).match(reg);
        return r?decodeURIComponent(r[2]):null;
    }
  });
})(jQuery);