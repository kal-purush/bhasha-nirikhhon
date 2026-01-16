// ページトップ
jQuery( function() {
  var $offset = jQuery( '#nav' ).offset(),
      navHeight = jQuery( '#nav' ).outerHeight();
  jQuery( window ).scroll( function () {
    if( jQuery( window ).scrollTop() > $offset.top ) {
      jQuery( '#nav' ).addClass( 'fixedNav' );
      $( 'body' ).css('padding-top', navHeight);
      $('#page_top').stop().animate({
        'right': '50px' //右から0pxの位置に
      }, 300);
    } else {
      jQuery( '#nav' ).removeClass( 'fixedNav' );
      $( 'body' ).css('padding-top', 0);
      $('#page_top').stop().animate({
        'right': '-50px'
      }, 300);
    }
  } );
} );
$('#page_top').click(function () {
    $('body, html').animate({ scrollTop: 0 }, 500);
    return false;
});

// ヘッダースムーススクロール
$(function() {
   $('a[href^="#"]').click(function(){
    var href = $(this).attr('href');
    var target = $(href == '#' || href === '' ? 'html' : href);
    var position = target.offset().top-100;
    
    $('html,body').animate({scrollTop : position}, 500);
  });
});

// マーカー
$(window).scroll(function (){
  $(".marker-animation").each(function(){
    var position = $(this).offset().top;
    var scroll = $(window).scrollTop();
    var windowHeight = $(window).height();
    
    if (scroll > position - windowHeight){
      $(this).addClass('active');
    }
  });
});