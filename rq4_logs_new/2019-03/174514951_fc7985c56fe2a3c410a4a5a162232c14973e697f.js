$(document).ready(function(){ 
    (function(){ 
        var i = 0; 
        setInterval(function(){ 
            $("body").removeClass("bg1, bg2, bg3, bg4").addClass("bg"+(i++%4 + 1)); 
        }, 10000); 
    })(); 
});