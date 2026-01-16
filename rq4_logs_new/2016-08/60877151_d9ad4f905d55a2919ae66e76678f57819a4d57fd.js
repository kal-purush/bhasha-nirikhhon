$(function(){

  $(window).keypress(function(e){ //detect the keypress

    // console.log(e); // see the details of the entire keypress event


    if(e.key == 'a'){ //are we getting an 'a'? if so, do something.
      console.log("key a pressed"); //log the information to the console window in our browser
    }

    if(e.key == 'b'){
      console.log("key b pressed");
    }

    if(e.key == 'c'){
      console.log("key c pressed");
    }

    if(e.key == '1'){
      console.log("key 1 pressed");
    }

    if(e.key == '2'){
      console.log("key 2 pressed");
    }

    if(e.key == '3'){
      console.log("key 3 pressed");
    }

  })


})