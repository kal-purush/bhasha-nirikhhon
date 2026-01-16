$(document).on('page:change',function(){
      var rotcounter=0;
  	 $('.zoomin').on('mousedown',function() {
      
      
      var newheight= $('.image_left img').height()+50;
      $('.image_left img').css({
        "max-height":"200%","max-width": "1000%" , "width":"auto"});
      $('.image_left img').height(newheight);
    });
  	 $('.zoomout').on('click',function() {
     
       
      
      var newheight = $('.image_left img').height()-50;
       $('.image_left img').css({
        "max-height":"200%","max-width": "1000%", "width":"auto"});
    
      $('.image_left img').height(newheight);
    });
  	 
  	 $('.reset').on('click',function() {
     
     $('.image_left img').css({
       	"max-height":"100%","max-width": "100%" , "height":"auto", "width":"auto"});
      
     
    });
  	$('.rotateright').on('click',function() {
      rotcounter+=90;
      if(rotcounter==360){rotcounter=0;}
  	$('.image_left img').css({

       	 '-webkit-transform': 'rotate('+rotcounter+'deg)',  //Safari 3.1+, Chrome  
         '-moz-transform': 'rotate('+rotcounter+'deg)',     //Firefox 3.5-15  
         '-ms-transform': 'rotate('+rotcounter+'deg)',      //IE9+  
         '-o-transform': 'rotate('+rotcounter+'deg)',       //Opera 10.5-12.00  
         'transform': 'rotate('+rotcounter+'deg)'  });

      
    });
    $('.rotateleft').on('click',function() {
      rotcounter-=90;
      if(rotcounter==-360){rotcounter=0;}
    $('.image_left img').css({

        '-webkit-transform': 'rotate('+rotcounter+'deg)',  //Safari 3.1+, Chrome  
        '-moz-transform': 'rotate('+rotcounter+'deg)',     //Firefox 3.5-15  
        '-ms-transform': 'rotate('+rotcounter+'deg)',      //IE9+  
        '-o-transform': 'rotate('+rotcounter+'deg)',       //Opera 10.5-12.00  
        'transform': 'rotate('+rotcounter+'deg)'  });

      
    });



 

 
});