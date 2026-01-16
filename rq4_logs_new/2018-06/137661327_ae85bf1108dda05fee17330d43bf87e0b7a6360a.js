var p = document.querySelector('.app-download-popup');
if(p != null) {
    p.style.display = 'none';
}

$('.slide-navi').click(function(){
    var attrs = $(this).find('a');
    var jsonObj = [];
    attrs.each(function(){
        jsonObj.push({ 'title': this.text, 'urlString': this.href });
});

webkit.messageHandlers.slideNavi.postMessage(JSON.stringify(jsonObj)); });