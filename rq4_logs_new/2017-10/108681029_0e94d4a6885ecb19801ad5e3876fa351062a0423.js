// ==UserScript==
// @name cover downloads
// @description for download images from bookstore
// @author Jaha Ereshov
// @license MIT
// @version 1.0
// @include https://bookstore.usip.org/books/*
// ==/UserScript==
// [1] Оборачиваем скрипт в замыкание, для кроссбраузерности (opera, ie)
(function (window, undefined) {  // [2] нормализуем window
    var w;
    if (typeof unsafeWindow != undefined) {
        w = unsafeWindow
    } else {
        w = window;
    }
    // В юзерскрипты можно вставлять практически любые javascript-библиотеки.
    // Код библиотеки копируется прямо в юзерскрипт.
    // При подключении библиотеки нужно передать w в качестве параметра окна window
    // Пример: подключение jquery.min.js
    // (function(a,b){function ci(a) ... a.jQuery=a.$=d})(w);

    // [3] не запускаем скрипт во фреймах
    // без этого условия скрипт будет запускаться несколько раз на странице с фреймами
    if (w.self != w.top) {
        return;
    }
    // [4] дополнительная проверка наряду с @include
    if (/https:\/\/bookstore.usip.org\/books\/*/.test(w.location.href)) 
    {
        function download(uri, name) 
        {
          var link = document.createElement("a");
          link.download = name;
          link.href = uri;
          document.body.appendChild(link);
          link.click();
          document.body.removeChild(link);
            delete link
        }
        
        let book = document.getElementsByClassName("book");
        for (let cover in book)
        {
            if(isNumeric(cover))
            {
                let highSrc = '';
                let coverDiv = book[cover].getElementsByClassName('book__cover-img');
                if(coverDiv[0] != undefined){
                    let midSrc = coverDiv[0].currentSrc;
                    if(typeof(midSrc) == 'string')
                    {
                        highSrc = midSrc.replace('cf200', 'cf300');
                        let isbnDiv = book[cover].getElementsByClassName('book__isbn');
                        if(isbnDiv[0] != undefined)
                        {
                            let isbn13Label = isbnDiv[0].innerText;
                            let isbn13 = '';
                            if(typeof(isbn13Label) == 'string')
                            {
                                isbn13 = isbn13Label.replace(/-/g, '');
                                let name = isbn13 + '.' + highSrc.split('.').pop();
                                download(highSrc, name);
                                setTimeout(1000);
                            }
                        }
                    }   
                }
            }
        }

        function isNumeric(n) {
            return !isNaN(parseFloat(n)) && isFinite(n);
        }
    }
})(window);

// https://bookstore.usip.org/sites/usip/images/covers/1878379003_cf300.jpg
// https://bookstore.usip.org/sites/usip/images/covers/1878379003_cf200.jpg

