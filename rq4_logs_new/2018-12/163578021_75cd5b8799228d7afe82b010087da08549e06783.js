chrome.browserAction.onClicked.addListener(function(tab) {
    chrome.tabs.getSelected(null, function(tab) {
        var text_area = document.createElement('textarea');
        //日付の取得
        var dt = new Date();
        var year = dt.getFullYear();
        var month = dt.getMonth() + 1;
        var date = dt.getDate();

        text_area.value = "・「" + tab.title + "」(" + year + "年" + month + "月" + date + "日確認)" + "\n" + tab.url;
        document.body.appendChild(text_area);

        text_area.select();
        document.execCommand('copy');

        document.body.removeChild(text_area);
    });
});