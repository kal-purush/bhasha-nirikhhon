var chatBox  = document.getElementsByName("message_body")[0];
var replyBut = document.getElementById("u_0_s");
 
var kitInt   = setInterval(function() {
    chatBox.classList.remove("DOMControl_placeholder")
    chatBox.value = new Date().getHours() + ":"+ new Date().getMinutes();
    replyBut.click();
 
}, 6000);
 
var stopKittens = function() { clearInterval(kitInt); };