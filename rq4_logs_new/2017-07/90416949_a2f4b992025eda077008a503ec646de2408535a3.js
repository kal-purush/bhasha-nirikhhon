/*Turn menu burger into a close 'X'*/
$(".menu-burger").click(function(){
    $("span:nth-child(1)").toggleClass("active-1");
    $("span:nth-child(2)").toggleClass("active-2");
    $("span:nth-child(3)").toggleClass("active-3");
});

$(".mobile-menu").hide();

$(".menu-burger").click(function(){
    $(".mobile-menu").slideToggle(500);
});

/* To-Top button */
// When the user scrolls down 20px from the top of the document, show the button
window.onscroll = function() {scrollFunction()};

function scrollFunction() {
    if (document.body.scrollTop > 200 || document.documentElement.scrollTop > 20) {
        document.getElementById("topBtn").style.display = "block";
    } else {
        document.getElementById("topBtn").style.display = "none";
    }
}

// When the user clicks on the button, scroll to the top of the document
function topFunction() {
    document.body.scrollTop = 0; // For Chrome, Safari and Opera 
    document.documentElement.scrollTop = 0; // For IE and Firefox
}