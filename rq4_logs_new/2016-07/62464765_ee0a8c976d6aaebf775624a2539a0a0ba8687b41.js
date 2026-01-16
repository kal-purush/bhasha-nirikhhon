$(document).ready(function(){

  $.get('http://api.giphy.com/v1/gifs/random?api_key=dc6zaTOxFJmzC')
  .then(function(ob){
    $('body').append('<img src='+ ob.data.image_url +'>');
    console.log(ob.data);
  })
  .catch;


});