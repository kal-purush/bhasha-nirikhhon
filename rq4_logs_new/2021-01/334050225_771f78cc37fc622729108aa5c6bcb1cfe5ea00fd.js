$('document').ready(function(){
    var cardWidth = $('div.cardsBlock > div.cards > div.card').width();
    var cardsWidth = $('div.cardsBlock > div.cards').width();
    var opacity_1 = parseInt(cardsWidth/cardWidth)-1;
    var cards = document.getElementsByClassName('card');
    for(var i = 1 ; i <= opacity_1 ; i++){
        cards[i+1].style.opacity = 1;
    }
    $('.card').css('left',`${parseFloat($('.card').css('left')) - cardWidth - 30}px`)

    //拉動銀幕
    $(window).resize(function(){
        var cardWidth = $('div.cardsBlock > div.cards > div.card').width();
        var cardsWidth = $('div.cardsBlock > div.cards').width();
        var opacity_1 = parseInt(cardsWidth/cardWidth)-1;
        var cards = document.getElementsByClassName('card');
        $('.card').css('left',`${0 - (cardWidth/2) - cardWidth - 30 }px`);
        for(var i = 0 ; i < cards.length ; i++){
            cards[i].style.opacity = 0.3;
        }
        for(var i = 0 ; i < opacity_1 ; i++){
            cards[i+2].style.opacity = 1;
        }
    });

    //左邊按鈕
    var isclick = true;
    $('div.cardsBlock > button.right').click(rightArrow);
    function rightArrow(){
        if(isclick){
            isclick = false;
                cardWidth = $('div.cardsBlock > div.cards > div.card').width();
                cardsWidth = $('div.cardsBlock > div.cards').width();
                cardLeft = parseFloat($('div.cardsBlock > div.cards > div.card').css('left')) ;
                cards = document.getElementsByClassName('card');
                opacity_1 = parseInt(cardsWidth/cardWidth)-1;
        
                var arr = [];
                for(var i = 0 ; i < cards.length ; i++){
                    if(cards[i].style.opacity == 1){
                    arr.push(i);
                    }
                };
                var cardMinIndex = arr[0];
                var cardMaxIndex = arr[opacity_1-1];
                if(cardMaxIndex <= cards.length-3){
                    cards[cardMinIndex].style.opacity = 0.3;
                    cards[cardMaxIndex+1].style.opacity = 1;
                    // alert(`${min} ${max}`);
                    $('div.cardsBlock > div.cards > div.card').animate({left: cardLeft-cardWidth-30});
                }
                setTimeout(function(){
                    isclick = true;
                },500);
        }
    }

    //右邊按鈕
    $('div.cardsBlock > button.left').click(leftArrow);
    function leftArrow(){
        if(isclick){
            isclick = false;
            cardWidth = $('div.cardsBlock > div.cards > div.card').width();
            cardsWidth = $('div.cardsBlock > div.cards').width();
            cardLeft = parseFloat($('div.cardsBlock > div.cards > div.card').css('left')) ;
            cards = document.getElementsByClassName('card');
            opacity_1 = parseInt(cardsWidth/cardWidth)-1;
    
            var arr = [];
            for(var i = 0 ; i < cards.length ; i++){
                if(cards[i].style.opacity == 1){
                   arr.push(i);
                }
            };
            var cardMinIndex = arr[0];
            var cardMaxIndex = arr[opacity_1-1];
            if(cardMinIndex > 1){
                cards[cardMinIndex-1].style.opacity = 1;
                cards[cardMaxIndex].style.opacity = 0.3;
                // alert(`${min} ${max}`);
                $('div.cardsBlock > div.cards > div.card').animate({left: cardLeft+cardWidth+30});
            }
            setTimeout(function(){
                isclick = true;
            },500);
        }
    }
})