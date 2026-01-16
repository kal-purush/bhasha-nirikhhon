$(document).load($(window).bind(pupsik));

    function pupsik()
    {
        if ($('.with-summary','book'))
        {
          if($(window).width() < 820px)
          {
              $(".text").css({'visibility': 'hidden', 'width': '1px','height':'1px'});
              $(".btn-knopka").css({'display': 'block', 'position': 'relative', 'width': '140px', 'height': '30px', 'text-align': 'center', 'top': '6px', 'margin': '4px auto'});
          }
      }
    }