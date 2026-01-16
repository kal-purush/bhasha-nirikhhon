/**
 * Created by PengZong on 16/11/23.
 */

$('.scroll-up-div').click(function () {
    //获取当前对象的父级对象
    var card= $(this).parent();
    var card_id = card[0]['id'];  //获取父级对象的id
    var _id = card_id.substring(4);

    var card_num = document.getElementsByClassName("scroll-up-div");
    var _num = card_num.length;
    setTimeout(function() {
        $('#'+card_id).removeClass("card");
        $('#'+card_id).addClass("cardn");

        if(_id == _num){
            $('#content')[0].style.display="block";
        }
    }, 100);
});


$('.scroll-down-div').click(function () {
    //获取当前对象的父级对象
    var card= $(this).parent();
    var card_id = card[0]['id'];  //获取父级对象的id

    var _id = card_id.substring(4) - 1;

    setTimeout(function() {
        $('#card'+_id).removeClass("cardn");
        $('#card'+_id).addClass("card");
    }, 100);
});

function backhome() {
    var card_num = document.getElementsByClassName("scroll-up-div");

    for(var i=1;i<=card_num.length;i++){
        var _id = String(i);
        setTimeout(function() {
            $('#card'+_id).removeClass("cardn");
            $('#card'+_id).addClass("card");
        }, 100);
    }

    $('#content')[0].style.display="none";
}