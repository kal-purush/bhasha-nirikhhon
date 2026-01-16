'use-strict'
$(document).ready(function(){
  addCoinListeners();
  addBeginListener();
})

function addCoinListeners(){
  $("[id*='coin']").click(function(coin){
    var coin_id = $(this).attr('id');
    
  });
}

function addBeginListener(){
  $('#begin').click(function(){
    $('#begin').attr('disabled', 'disabled');
    $('#coin1').removeAttr('disabled');
    $('#coin1').removeClass('btn btn-default');
    $('#coin1').addClass('btn btn-success');
  });
}

function tossCoin(){
  var next = this.attr("id");

}

// Reading.prototype.tossCoin = function(coin_num){
//   var current = "#coin"+coin_num;
//   var next = "#coin"+(coin_num+1);

//   $(current).attr('disabled','disabled');
//   $(current).removeClass('btn btn-success');
//   $(current).addClass('btn btn-success');
//   $(next).removeAttr('disabled');
//   $(next).removeClass('btn btn-default');
//   $(next).addClass('btn btn-success');

// }