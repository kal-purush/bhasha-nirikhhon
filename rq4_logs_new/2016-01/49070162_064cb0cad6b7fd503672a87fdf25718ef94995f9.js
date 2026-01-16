$(document).ready(function () {
   $("img").each(function () {
        var $this = $(this);
    
        this.onerror = function() {
            $this.hide();
        };
    });
   
   function replaceimg(img) {
       img.attr("src","broken.jpg");
   } 
   
   $('.grid').masonry({
      // options...
      itemSelector: '.grid-item',
      columnWidth: 200
    });
});
