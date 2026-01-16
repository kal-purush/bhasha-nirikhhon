$(window).resize(function() { if($(window).width() >481) {
  $(document).ready(function() {
    /* 1 */
    $(window).scroll( function(){
        
      // container_1 스크롤할 경우 이벤트
        $('#container_1').each( function(i){
            var bottom_of_object = $(this).offset().top + $(this).outerHeight();
            var bottom_of_window = $(window).scrollTop() + $(window).height();
            /* 3 */
            if( bottom_of_window > bottom_of_object/3 ){
                //$(this).animate({'opacity':'1'},5);
                $('#seoulGraph').animate({'width':'300px'},2000);
                $('#percentage').animate({'opacity':'1'},2500);
                $('#noGraph').animate({'width':'380px'},1500);
                $('#no_percentage').animate({'opacity':'1'},2500);
            }
        }); 
  
        // container_2 스크롤할 경우 이벤트
        $('#container_2').each( function(i){
          var bottom_of_object = $(this).offset().top + $(this).outerHeight();
          var bottom_of_window = $(window).scrollTop() + $(window).height();
          /* 3 */
          if( bottom_of_window > bottom_of_object/1.5 ){
              //$(this).animate({'opacity':'1'},5);
              $('#dot_1').animate({'opacity':'1'},500);
              $('#dot_2').animate({'opacity':'1'},1000);
              $('#dot_3').animate({'opacity':'1'},1500);
              $('#dot_6').animate({'opacity':'1'},2000);
              $('#dot_5').animate({'opacity':'1'},2500);
              $('#dot_4').animate({'opacity':'1'},3000);
          }
      }); 
  
  
        // container_3 스크롤할 경우 이벤트
        $('#container_3').each( function(i){
          var bottom_of_object = $(this).offset().top + $(this).outerHeight();
          var bottom_of_window = $(window).scrollTop() + $(window).height();
          /* 3 */
          if( bottom_of_window > bottom_of_object/1.5 ){
              $('#whyilsan').animate({'opacity':'1'},500);
              $('#whyilsan_2').animate({'opacity':'1'},1000);
              $('#whyilsan_3').animate({'opacity':'1'},2000);
          }
      }); 
  
      $('#container_2_subtitle').each( function(i){
        var bottom_of_object = $(this).offset().top + $(this).outerHeight();
        var bottom_of_window = $(window).scrollTop() + $(window).height();
        /* 3 */
        if( bottom_of_window > bottom_of_object ){
            $('#container_2_subtitle').animate({'opacity':'1'},500);
        }
    }); 
  
  
  
    });
  });
}
});

if($(window).width() >481) {
    $(document).ready(function() {
      /* 1 */
      $(window).scroll( function(){
          
        // container_1 스크롤할 경우 이벤트
          $('#container_1').each( function(i){
              var bottom_of_object = $(this).offset().top + $(this).outerHeight();
              var bottom_of_window = $(window).scrollTop() + $(window).height();
              /* 3 */
              if( bottom_of_window > bottom_of_object/3 ){
                  //$(this).animate({'opacity':'1'},5);
                  $('#seoulGraph').animate({'width':'300px'},2000);
                  $('#percentage').animate({'opacity':'1'},2500);
                  $('#noGraph').animate({'width':'380px'},1500);
                  $('#no_percentage').animate({'opacity':'1'},2500);
              }
          }); 
    
          // container_2 스크롤할 경우 이벤트
          $('#container_2').each( function(i){
            var bottom_of_object = $(this).offset().top + $(this).outerHeight();
            var bottom_of_window = $(window).scrollTop() + $(window).height();
            /* 3 */
            if( bottom_of_window > bottom_of_object/1.5 ){
                //$(this).animate({'opacity':'1'},5);
                $('#dot_1').animate({'opacity':'1'},500);
                $('#dot_2').animate({'opacity':'1'},1000);
                $('#dot_3').animate({'opacity':'1'},1500);
                $('#dot_6').animate({'opacity':'1'},2000);
                $('#dot_5').animate({'opacity':'1'},2500);
                $('#dot_4').animate({'opacity':'1'},3000);
            }
        }); 
    
    
          // container_3 스크롤할 경우 이벤트
          $('#container_3').each( function(i){
            var bottom_of_object = $(this).offset().top + $(this).outerHeight();
            var bottom_of_window = $(window).scrollTop() + $(window).height();
            /* 3 */
            if( bottom_of_window > bottom_of_object/1.5 ){
                $('#whyilsan').animate({'opacity':'1'},500);
                $('#whyilsan_2').animate({'opacity':'1'},1000);
                $('#whyilsan_3').animate({'opacity':'1'},2000);
            }
        }); 
    
        $('#container_2_subtitle').each( function(i){
          var bottom_of_object = $(this).offset().top + $(this).outerHeight();
          var bottom_of_window = $(window).scrollTop() + $(window).height();
          /* 3 */
          if( bottom_of_window > bottom_of_object ){
              $('#container_2_subtitle').animate({'opacity':'1'},500);
          }
      }); 
    
    
    
      });
    });
  }

$(window).resize(function() { if($(window).width() <=481) {
  $(document).ready(function(){
    $('#whyilsan').animate({'opacity':'1'});
    $('#whyilsan_2').animate({'opacity':'1'});
    $('#whyilsan_3').animate({'opacity':'1'});
    $('#percentage').animate({'opacity':'1'});
    $('#no_percentage').animate({'opacity':'1'});
    $('#noGraph').css('width','230px');
    $('#noGraph').css('width','400px');
    $('#container_2_subtitle').css('opacity','1');
  });
}
});

if($(window).width() <=481) {
  $(document).ready(function(){
    $('#whyilsan').animate({'opacity':'1'});
    $('#whyilsan_2').animate({'opacity':'1'});
    $('#whyilsan_3').animate({'opacity':'1'});
    $('#percentage').animate({'opacity':'1'});
    $('#no_percentage').animate({'opacity':'1'});
    $('#noGraph').css('width','230px');
    $('#noGraph').css('width','400px');
    $('#container_2_subtitle').css('opacity','1');
  });
}




var slider = document.getElementById('slider_id');
var slider_height = document.getElementById('slider_id').offsetHeight;

function timer() {
  

  if (slider_height == 120) {
    slider_height = 0;
    plusDivs(1);
    slider.style.height = "0px";
    
  }else if (slider_height < 120) {
    slider.style.height = slider_height + 1 + 'px';
    slider_height = document.getElementById('slider_id').offsetHeight;
  }
}
  setInterval("timer()", 40);



  var slideIndex = 1;
  showDivs(slideIndex);

  function plusDivs(n) {
    showDivs(slideIndex += n);
    slider_height = 0;
  }

  

  function showDivs(n) {
    var size;
    if (window.innerWidth <= 420) {
      size = window.innerWidth;
    }else {
      size = 950;
    }
    var i;
    var wraps = document.getElementById("sliderImage")
    var x = document.getElementsByClassName("mySlides");
    var x_title = document.getElementsByClassName("slidesName");

    if (n > x.length) { slideIndex = 1 }
    if (n < 1) { slideIndex = x.length };
    for (i = 0; i < x.length; i++) {
      x_title[0].style.opacity = "1";
    }
    // x[i].style.marginLeft = "-950px";
    if (slideIndex - 1 == 0) {
      x_title[0].innerHTML = "1970년대 일산 사진관 앞";
      wraps.style.marginLeft = -size * 0 + "px";
    }
    if (slideIndex - 1 == 1){
      x_title[0].innerHTML = "개발 전 일산역 모습";
      wraps.style.marginLeft = -size * 1 + "px";
    } 
    if (slideIndex - 1 == 2) {
      x_title[0].innerHTML = "일산시장";
      wraps.style.marginLeft = -size * 2 + "px";
    }      
    if (slideIndex - 1 == 3){
      x_title[0].innerHTML = "일산읍 모습 ";
      wraps.style.marginLeft = -size * 3 + "px";
    } 
  }



var viewmore = document.getElementById("viewmoreInner")
var viewmore_height = document.getElementById('viewmoreInner').offsetHeight;



function viewMore() {
  if (viewmore_height < 150){
    viewmore.style.height = viewmore_height + 1 + 'px';
    viewmore_height = document.getElementById('viewmoreInner').offsetHeight;
  } else if (viewmore_height == 150){
    viewmore_height = 0;
    viewmore.style.height = "0px";

  }
}

setInterval("viewMore()", 50);


// onclick

function goto(url) {
  window.location = url;

}
