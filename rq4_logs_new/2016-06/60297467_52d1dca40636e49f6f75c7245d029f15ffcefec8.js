// This is a manifest file that'll be compiled into application.js, which will include all the files
// listed below.
//
// Any JavaScript/Coffee file within this directory, lib/assets/javascripts, vendor/assets/javascripts,
// or any plugin's vendor/assets/javascripts directory can be referenced here using a relative path.
//
// It's not advisable to add code directly here, but if you do, it'll appear at the bottom of the
// compiled file.
//
// Read Sprockets README (https://github.com/rails/sprockets#sprockets-directives) for details
// about supported directives.
//
//= require jquery
//= require jquery_ujs
//= require turbolinks
//= require_tree .

$(function(){
  $('.glyphicon-plus').click(function() {
    var el = $(this).parents('.item').find('input')
    var quantity = parseInt(el.val());
    quantity++;
    el.val(quantity);
  });
  $('.glyphicon-minus').click(function() {
    var el = $(this).parents('.item').find('input')
    var quantity = parseInt(el.val());
    quantity--;
    if (quantity < 0) {
      quantity = 0;
    }
    el.val(quantity);
  });

  if ($('body').is('.orders.index')) {
    $('body').keyup(function(event){
      if (event.keyCode == 13){
        if($('.btn').size() > 0) {
          document.location = $('.btn').attr('href');
        }else{
          document.location = document.location;
        }
      }
    });
  }
});