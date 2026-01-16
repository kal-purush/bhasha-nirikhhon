var iScrollPos = 0;
$(window).scroll(function() {    
    var scroll = $(window).scrollTop();
    var dynamicScroll = scroll;
    /*console.log('scroll',scroll);
    console.log('dynamicScroll',dynamicScroll);*/

    /*if (scroll >= 100) {
        for(var i = 5; i < 50; i++){
            setTimeout(function(){ 
                console.log('i',i);
                $(".rabbit").css("top", i+"%");
            }, 3000);
        }
    }*/
    /*
    if(scroll === 600){
        dynamicScroll = 0;
    }*/

    var iCurScrollPos = $(this).scrollTop();
    if (iCurScrollPos > iScrollPos) {
        moveRabbit (scroll);
    } else {  
        $(".rabbit").css('top', '0%');
    }

    iScrollPos = iCurScrollPos;          

});
        
function moveRabbit (scroll) {
    if ((scroll >= 100 && scroll < 200) || (scroll >= 700 && scroll < 800) || (scroll >= 1300 && scroll < 1400) || (scroll >= 1900 && scroll < 2000) || (scroll >= 2500 && scroll < 2600) || (scroll >= 3100 && scroll < 3200) || (scroll >= 3700 && scroll < 3800) || (scroll >= 4300 && scroll < 4400) || (scroll >= 4900 && scroll < 5000) || (scroll >= 5500 && scroll < 5600)) {
        $(".rabbit").css('top', '8%');
    }
    else if ((scroll >= 200 && scroll < 300) || (scroll >= 800 && scroll < 900) || (scroll >= 1400 && scroll < 1500) || (scroll >= 2000 && scroll < 2100) || (scroll >= 2600 && scroll < 2700) || (scroll >= 3200 && scroll < 3300) || (scroll >= 3800 && scroll < 3900) || (scroll >= 4400 && scroll < 4500) || (scroll >= 5000 && scroll < 5100) || (scroll >= 5600 && scroll < 5700)) {
        $(".rabbit").css('top', '16%');
    }
    else if ((scroll >= 300 && scroll < 400) || (scroll >= 900 && scroll < 1000) || (scroll >= 1500 && scroll < 1600) || (scroll >= 2100 && scroll < 2200) || (scroll >= 2700 && scroll < 2800) || (scroll >= 3300 && scroll < 3400) || (scroll >= 3900 && scroll < 4000) || (scroll >= 4500 && scroll < 4600) || (scroll >= 5100 && scroll < 5200) || (scroll >= 5700 && scroll < 5800)) {
        $(".rabbit").css('top', '24%');
    }
    else if ((scroll >= 400 && scroll < 500) || (scroll >= 1000 && scroll < 1100) || (scroll >= 1600 && scroll < 1700) || (scroll >= 2200 && scroll < 2300) || (scroll >= 2800 && scroll < 2900) || (scroll >= 3400 && scroll < 3500) || (scroll >= 4000 && scroll < 4100) || (scroll >= 4600 && scroll < 4700) || (scroll >= 5200 && scroll < 5300)) {
        $(".rabbit").css('top', '32%');
    }
    else if ((scroll >= 500 && scroll < 600) || (scroll >= 1100 && scroll < 1200) || (scroll >= 1700 && scroll < 1800) || (scroll >= 2300 && scroll < 2400) || (scroll >= 2900 && scroll < 3000) || (scroll >= 3500 && scroll < 3600) || (scroll >= 4100 && scroll < 4200) || (scroll >= 4700 && scroll < 4800) || (scroll >= 5300 && scroll < 5400)) {
        $(".rabbit").css('top', '40%');
    }
    else {
        $(".rabbit").css('top', '0%');
    }
}