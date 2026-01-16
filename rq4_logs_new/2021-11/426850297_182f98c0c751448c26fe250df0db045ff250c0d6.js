// JavaScript Document

(function($) {
  $(function() {
    
    // スムーズスクロール設定
    const scrollSpeed = 1000; // スクロール時間（ミリ秒）
    const fixedHeaderOffset = false; // 固定ヘッダー用スクロール量調整ON/OFF（true/false）
    const additionalOffset = 0; // スクロール量調整（px）
    // トップへ戻るボタン設定
    const buttonOffset = 200; // トップへ戻る固定ボタンを表示するスクロール量（px）

    
    // スムーズスクロール
    $('a[href^="#"]').on('click', function() {
      const href= $(this).attr('href');
      const headerHeight = (fixedHeaderOffset) ? $('header').outerHeight() : 0;
      if (href !== '#' || href !== '') {
        const target = (href === '#top' && ! $('#top').length) ? 'html' : href;
        const position = $(target).offset().top - headerHeight - additionalOffset;
        $('body, html').animate({scrollTop: position}, scrollSpeed, 'swing');
        return false;
      }
    });
    // トップへ戻る固定ボタンの表示・非表示
    const sfBtn = $('.scroll-fade');
    if (sfBtn.length) {
      $(sfBtn).css('display', 'none');
      $(window).on('scroll', function() {
        const sfBtnDisplay = sfBtn.css('display');
        if ($(this).scrollTop() > buttonOffset && sfBtnDisplay === 'none') {
          sfBtn.stop().fadeIn();
        } else if ($(this).scrollTop() <= buttonOffset && sfBtnDisplay !== 'none')  {
          sfBtn.stop().fadeOut();
        }
      });
    }
		
  });
})(jQuery);