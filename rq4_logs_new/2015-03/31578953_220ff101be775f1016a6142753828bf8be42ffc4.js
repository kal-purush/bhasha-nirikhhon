var master = {window: {el: null, width: null, height: null}};


function calc() {
    master.window.height = $(window).height();
    master.window.width = $(window).width();

    $content = master.window.height - 112;
    $container = master.window.width - 385;
    $videoList = $content - 52;

    if (master.window.width >= 950) {
        $('.container').width($container);
        $('body').removeClass('mobile');
        $('.sidebar').show();
    } else if (master.window.width < 950) {
        $('.container').width('100%');
        $('body').addClass('mobile');
        $('.sidebar').hide();

    }
    $('.content').height($content);
    $('.videoList').height($videoList);

    drawPage();
}
function drawPage() {
    $('.videoList>ul>li').first().addClass('first');
    $('.videoList>ul>li').last().addClass('last');

    afterLoaded();
}
function actions() {
    cheet('s h a k e s p e a r e', function () {
      alert('Doubt thou the stars are fire; \nDoubt that the sun doth move; \nDoubt truth to be a liar; \nBut never doubt I love. \n                                                        - William Shakespeare');
    });

    $('.videoList>ul>li').each(function() {
        var id = $(this).data('hash'),
            timer = $(this).find('.timing span'),
            titler = $(this).find('.name'),
            ytapiurl    = 'http://gdata.youtube.com/feeds/api/videos/' + id + '?alt=json';

        $.getJSON(ytapiurl, function(data) {
            var time = formatSecondsAsTime(data['entry']['media$group']['media$content'][0]['duration']);
            var title = data['entry']['media$group']['media$title']['$t'];

            timer.text(time);
            titler.text(title.replace('Você Não Sabia? - ', ''));
        });

        $(this).removeClass('invisible');
        $(this).addClass('fadeInUp animated');


        var elVideoID = $(this).data('hash');
        if ($(this).hasClass('current')) {

            putItOnDOM(elVideoID);

        } else {
            var elVideoID2 = $('.videoList>ul>li').eq('0').data('hash');
            $('.videoList>ul>li').eq('0').addClass('active');

            putItOnDOM(elVideoID2);

        }

    });

    $('.videoList ul li').click(function() {
        if ($(this).hasClass('active')){
            return false;
        } else {
            $('.videoList>ul>li.active').addClass('watched');
            $('.videoList>ul>li').removeClass('active');
            $(this).addClass('active');
            var videoID = $(this).data('hash');
            putItOnDOM(videoID);
        }
    });


    startScroller();

    $('#down').click(nextItem);
    $('#up').click(prevItem);


    $('#menuM').click(function() {
        $('.sidebar').fadeToggle();
    });
    $('#prevV').click(function() {
        if ($('.videoList>ul>li.active').hasClass('first')) {
            return false;
        } else {
            var thisis = $('.videoList>ul>li.active').prev();
            thisis.addClass('prospect');
            $('.videoList>ul>li.active').addClass('watched');
            $('.videoList>ul>li').removeClass('active');
            $('.videoList>ul>li.prospect').addClass('active');
            $('.videoList>ul>li.active').removeClass('prospect');
            var nextVideoId = $('.videoList>ul>li.active').data('hash');
            isActive();
            putItOnDOM(nextVideoId);
        }
    });

    $('#nextV').click(function() {
        if ($('.videoList>ul>li.active').hasClass('last')) {
            return false;
        } else {
            var thisis = $('.videoList>ul>li.active').next();
            thisis.addClass('prospect');
            $('.videoList>ul>li.active').addClass('watched');
            $('.videoList>ul>li').removeClass('active');
            $('.videoList>ul>li.prospect').addClass('active');
            $('.videoList>ul>li.active').removeClass('prospect');
            var nextVideoId = $('.videoList>ul>li.active').data('hash');
            isActive();
            putItOnDOM(nextVideoId);
        }
    });

    function isActive() {
        if($('.videoList>ul>li.active').hasClass('last')) {
            $('#nextV').css('opacity','0.3');
        }
        if (!$('.videoList>ul>li.active').hasClass('last')) {
            $('#nextV').css('opacity','1');
        }
        if($('.videoList>ul>li.active').hasClass('first')) {
            $('#prevV').css('opacity','0.3');
        }
        if (!$('.videoList>ul>li.active').hasClass('first')) {
            $('#prevV').css('opacity','1');
        }

    }

}

function putItOnDOM(theVideoId) {
    $('.container .videoHolder').fadeOut('medium', function() {
                $('.container .videoHolder').html('<iframe id="mainVideo" width="100%" height="100%" src="https://www.youtube.com/embed/' + theVideoId + '?autoplay=1&showinfo=0" frameborder="0" allowfullscreen></iframe>');
                setTimeout(function() {
                    $('.container .videoHolder').fadeIn('medium');
                }, 400)
            });
}

function startScroller() {
    var itemsL = $('#scrollerMenu li').length * 42,
        videoListHeight = $('.videoList').height();
    if (itemsL < videoListHeight) {
        $('.videoCtrl').hide();
    } else {
        $('.videoCtrl').show();
        $('.videoCtrl #up').css('opacity','0.3');
        $('#scrollerMenu').css({
            top: 0
        });
    }
}
function nextItem() {
    var someTop = $('#scrollerMenu').css('top').replace('px','');
    var someHeight = $('#scrollerMenu').height();
    var otherHeight = $('.videoList').height();
    var diferHeight = otherHeight - someHeight;

    if (diferHeight > someTop) {
        $('.videoCtrl #down').css('opacity','0.3');
        return false;
    } else {
        $('.videoCtrl #up').css('opacity','1');
        $('#scrollerMenu').animate({
            top: "-=42"
        }, 200);
    }
}
function prevItem() {
    var someTop = $('#scrollerMenu').css('top').replace('px','');

    if (someTop >= 0) {
        $('.videoCtrl #up').css('opacity','0.3');
        return false;
    } else {
        $('.videoCtrl #down').css('opacity','1');
        $('#scrollerMenu').animate({
            top: "+=42"
        }, 200);
    }
}

function formatSecondsAsTime(secs) {
    var hr = Math.floor(secs / 3600);
    var min = Math.floor((secs - (hr * 3600)) / 60);
    var sec = Math.floor(secs - (hr * 3600) - (min * 60));

    if (hr < 10) {
        hr = "0" + hr;
    }
    if (min < 10) {
        min = "0" + min;
    }
    if (sec < 10) {
        sec = "0" + sec;
    }
    if (hr) {
        hr = "00";
    }
    if (hr == '00'){
        return min + ':' + sec;
    } else {
        return hr + ':' + min + ':' + sec;
    }
}

function afterLoaded() {
    $('.wrapper').fadeIn('medium');
}



$(window).load(calc);
$(document).ready(actions);
$(window).resize(calc).trigger('resize');