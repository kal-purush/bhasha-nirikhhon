$(document).ready(function () {

    $(".tr-clickable td[class!='td-button']").click(function () {
        var href = $(".tr-clickable").attr('href');
        window.location.href = href;
    });

});

function hideModal() {
    $('.modal-backdrop.fade.in').remove();
}