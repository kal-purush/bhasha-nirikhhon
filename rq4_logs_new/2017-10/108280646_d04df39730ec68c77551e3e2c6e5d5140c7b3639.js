(function(){
  var randao = {
    init: function(){
      var m = this;
      m.$roll = $('#rolling');
      m.$countryBox = $('.country-box');
      m.$navbarBtn = $('.navbar-toggle');
      m.$backtotop = $('.backtotop');
      m.$submenu = m.$countryBox.find('.is-dropdown-submenu');
      m.$win = $(window);

      m.initSwiper();
      m.pullRollCartoon();
      m.handle();
    },
    initSwiper: function () {
      new Swiper('.swiper-container', {
        navigation: {
          nextEl: '.swiper-button-next',
          prevEl: '.swiper-button-prev'
        },
        pagination: {
          el: '.swiper-pagination'
        },
        autoplay: {
          delay: 2500,
          disableOnInteraction: false,
        }
      });
    },
    handle: function () {
        var m = this;
        m.$countryBox.on('mouseover',function(){
          m.$submenu.show();
        }).on('mouseout', function(){
          m.$submenu.hide();
        });
        m.$submenu.find('li').on('click', function(){
          m.$submenu.hide();
        });

        m.$navbarBtn.on('click', function(){
            var $curDom = $($(this).attr('data-target'));
            $curDom.hasClass('in') 
            ? $curDom.removeClass('in')
            : $curDom.addClass('in');
        });

        m.$win.scroll(function() {
          var scrollPos = $(this).scrollTop();
          scrollPos > 100
          ? m.$backtotop.addClass('showme')
          : m.$backtotop.removeClass('showme');
        });

        m.$backtotop.on('click', function(){
          $('body,html').animate({
            scrollTop:0
          },500);
        });

    },
    pullRollCartoon: function () {
      var m = this;
          rollOffset = $('.animation1').offset().top + 100,
          m.$chatBox = $('.chat-box'),
          m.$chatList = $('.chat-list'),
          chatOffset = m.$chatBox.offset().top - 500,
          maxChatOffset = m.$chatBox[0].offsetHeight + chatOffset;
          isRoll = false,
          isChat = false;
          m.$win.scroll(function(){
            var scrollPos = $(this).scrollTop();
            if(scrollPos > rollOffset && ( scrollPos - rollOffset < 50)  && !isRoll){
               m.rolling();
               isRoll = true;
            };
            if(scrollPos - rollOffset > 500 && isRoll) isRoll = false;

            if(scrollPos > chatOffset && scrollPos < maxChatOffset && !isChat){
                m.$chatList.addClass('animation2');
                isChat = true;
            };
            if((scrollPos < chatOffset) && isChat){
                isChat = false;
                m.$chatList.removeClass('animation2');
            };
      });
    },
    rolling: function () {
      var m = this;
      var opts = {
          y: {
            num: 20,
            date: 2017
          },
          m: {
            num: 10,
            date: 9
          },
          d: {
            num: 20,
            date: 20
          },
          time: 80
      }
      m.$year = m.$roll.find('.year'),
      m.$month = m.$roll.find('.month'),
      m.$date = m.$roll.find('.date');
      var cartoon = function (type, obj) {
        var v = (opts[type].date / opts[type].num).toFixed();
            nYear = opts[type].date - v * opts[type].num,
            y = 0;
          obj.text(nYear)
          var yAn = setInterval(function(){
              y++;
              var nowY = nYear + y * v;
              if(nowY > opts[type].date){
                clearInterval(yAn);
                obj.text(opts[type].date);
                return false;
              };
              obj.text(nowY);
          }, opts.time);
      };
      cartoon('y', m.$year);
      cartoon('m', m.$month);
      cartoon('d', m.$date);
    }
  }
  randao.init();
})();