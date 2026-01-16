
let innerBody = document.getElementById("content")

innerBody.addEventListener("scroll", function(){
    let height = innerBody.scrollHeight - innerBody.clientHeight;
    let scrolled = (innerBody.scrollTop / height) * 100;
    document.querySelector(".progress-bar").style.height = scrolled + "%";
})

