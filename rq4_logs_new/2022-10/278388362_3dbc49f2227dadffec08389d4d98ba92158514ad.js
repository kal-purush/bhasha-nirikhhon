function delay(callback, ms) {
    var timer = 0;
    return function() {
      var context = this, args = arguments;
      clearTimeout(timer);
      timer = setTimeout(function () {
        callback.apply(context, args);
      }, ms || 0);
    };
}

$(document).ready(function () {
    $('input[type=number]').keyup(function () {
        calculateFutureSize();
    });

    $('#quotePrice').change(function () {
        calculateFutureSize();
    });

    $('#quoteSymbol').change(function () {
        getCoinPrice();
    });

    $('#coinName').keyup(delay(function (e) {
        getCoinPrice();
    }, 1000));
});

var getCoinPrice = function () {
    let quoteSymbol = $('#quoteSymbol').val();
    let coinName =  $('#coinName').val();

    if(coinName == '' || coinName == undefined) return;
   
    let url = 'https://fapi.binance.com/fapi/v1/ticker/price?symbol=' + coinName + quoteSymbol;
    
    $.ajax({
        url: url,
        type: 'GET',
        success: function(data){             
            $('#currentPrice').val(data.price);
            $('#stoplossPrice').val(data.price);
            
            $('#currentPrice').trigger("change");            
        },
        error: function(data) {
            $('#currentPrice').val(0);
            $('#stoplossPrice').val(0);
        }
    });

};

var calculateFutureSize = function () {
    let stoploss = $('#stoploss').val();    
    let stoplossPrice = $('#stoplossPrice').val();
    let currentPrice = $('#currentPrice').val();    

    if(currentPrice == stoplossPrice || currentPrice == 0 || stoplossPrice == 0){
        $('#volumeQuote').text(0);
        $('#volumeCoin').text(0);
        return;
    }
    let volumeCoin = stoploss / (currentPrice - stoplossPrice);
    let volumeQuote = volumeCoin * currentPrice;
    
    $('#volumeQuote').text(Number(volumeQuote).toFixed(2) + ' ' + $('#quoteSymbol').val());
    $('#volumeCoin').text(Number(volumeCoin).toFixed(2));

};