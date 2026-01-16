//.buttonがクリックされたら、(1).buttonに.actionを追加 / (2).navigation-contentsに.openを追加 / (3) sectionに.closeを追加する
jQuery(function () {
    jQuery('.hb-navigation .button').click(function () {
        jQuery(this).toggleClass('action');
        jQuery('.navigation-contents').toggleClass('open');
        jQuery('segment').toggleClass('close');
jQuery(document).ready(function(){
alert("jQueryが読み込まれたよ！やったね！");
});

    });
});