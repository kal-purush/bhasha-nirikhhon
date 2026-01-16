$(window).on("load", function () {
    var webStorage = function () {
        if (sessionStorage.getItem("access")) {
            $("#splash").hide();
            $(".splashbg").hide();
            $("body").addClass("appear");
            /*2回目以降アクセス時の処理*/
        } else {
            sessionStorage.setItem("access", "true");

            //ロゴを1.2秒でフェードアウトする記述
            $("#splash-logo").delay(1200).fadeOut("slow");
            //=====ここからローディングエリア（splashエリア）を1.5秒でフェードアウトした後に動かしたいJSをまとめる
            $("#splash")
                .delay(1500)
                .fadeOut("slow", function () {
                    //ローディングエリア（splashエリア）を1.5秒でフェードアウトする記述
                    $("body").addClass("appear"); //フェードアウト後bodyにappearクラス付与
                });
            //=====ここまでローディングエリア（splashエリア）を1.5秒でフェードアウトした後に動かしたいJSをまとめる
            //=====ここから背景が伸びた後に動かしたいJSをまとめたい場合は
            $(".splashbg").on("animationend", function () {
                //この中に動かしたいJSを記載
            });
            //=====ここまで背景が伸びた後に動かしたいJSをまとめる
        }
    };
    webStorage();
});

/*ハンバーガーボタン */
$(function () {
    $(".sp_btn, .sp_nav li").on("click", function () {
        $(".sp_nav").fadeToggle();
        $(".sp_btn").toggleClass("open");
    });
});