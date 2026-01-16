$(window).on("load", afventKode);


var koden = 420;


function afventKode() {
    $(".noEntry").hide();
    document.querySelector(".knap").addEventListener("click", tjekKoden);

}


function tjekKoden() {

    if ($(".inputfelt").val() != 420) {
        $(".noEntry").show();

    } else {
        window.location = "main.html";
    }
}