(function ($) {
    //получение значение параметра filter из адресной строки
    var urlParameter = GetURLParameter('filter');
    //буква, по которой осуществляется фильтрация
    var filterLetter = $('.letter-filter');
    //прилипающий родительский блок-контейнер
    var filterContainer = $('#alphabet-sticky-block');
    //значение смещения, при наступлении которого происходит прилипание контейнера
    var limit = $('.footer').offset().top - filterContainer.height() - 20;
    $(window).scroll(function () {
        //текущее значение смещения верхней границы страницы относительно верхней границы окна
        var windowTop = $(window).scrollTop();
        //пока верхняя границы страницы не совпадает с верхней границей окна
        if (windowTop != 0) {
            //устанавливаем прилипающему контейнеру фиксированную позицию
            filterContainer.css({position: 'fixed', top: '1rem'});
        }
        //если совпадает
        else {
            //устанавливаем статическое позиционирование
            filterContainer.css('position', 'static');
        }
        //пока значение граничного смещения меньше значения текущего скролла
        if (limit < windowTop) {
            //устанавливаем верхнию границу прилипающему блоку равную разнице между текущим скролом и граничным
            // значением
            var diff = limit - windowTop;
            filterContainer.css({top: diff});
        }
    });

    //установка и снятие стилей на выбранную букву(и соседние) при наведении на нее курсора
    filterLetter.hover(
        function () {
            $(this).addClass('hover');
            $(this).next().addClass('adjacent');
            $(this).prev().addClass('adjacent');
        },
        function () {
            $(this).removeClass('hover');
            $(this).next().removeClass('adjacent');
            $(this).prev().removeClass('adjacent');
        }
    );

    /*
    * функция получения параметров из адресной строки
     */
    function GetURLParameter(name)
    {
        name = name.replace(/[\[]/, '\\[').replace(/[\]]/, '\\]');
        var regex = new RegExp('[\\?&]' + name + '=([^&#]*)');
        var results = regex.exec(location.search);
        if (results === null) {
            results = '';
        } else {
            results = decodeURIComponent(results[1].replace(/\+/g, ' '));
        }
        return results;
    }

    //если при загрузке страницы в адресе присутствует параметр, то букве соответствующей его значению вешаем класс
    // active
    if (urlParameter) {
        $('.letter-filter:contains("'+urlParameter+'")').addClass('active');
    }

    //подгрузка контента отфильтрованного по выбранной букве
    filterLetter.on('click', function (e) {
        e.preventDefault();
        //выбранная буква
        var letter = $(this).html();
        //основной урл страницы(без параметров)
        var mainUrl = window.location.href.split('?')[0];
        //параметры урл
        var data = '';
        //урл, устанавливающийся в адресную строку браузера
        var historyUrl = mainUrl;
        //если выбранная буква не является текущей, по которой происходит фильтрация
        if (!$(this).hasClass('active')) {
            //снимаем с других букв класс активной
            $('.letter-filter').removeClass('active');
            //текущей букве устанавливаем класс активной
            $(this).addClass('active');
            //в пареметры урл записываем массив с необходимыми значениями
            data = {filter: letter};
            //в урл для адресной строки дописыаем парметр
            historyUrl = mainUrl + '?filter=' + letter;
        } else {
            //иначе убираем с текущей буквы класс активной
            $(this).removeClass('active');
        }
        $.ajaxSetup({
            headers: {'X-CSRF-TOKEN': $('meta[name="_token"]').attr('content')}
        });
        $.ajax({
            type: "GET",
            //урл по которому будет вызван соответсвующий контроллер
            url: mainUrl + '/filterLetter',
            data: data,
            beforeSend: function () {
                //перед отправкой аякс-запроса вешам спинер
                $('.page-content').addClass('spinner');
            },
            success: function (data) {
                //TODO: добавить уведомление об отсутсвии авторов с выбранной фильтрацией
                //меняем урл в адресной строке
                history.pushState('filter=' + letter, '', historyUrl);
                //заменяем в основном контейнере старый контент полученным
                $('#main-container').html(data);
                //убираем спинер
                $('.page-content').removeClass('spinner');
                //перерасчет смещения, при наступлении которого происходит прилипание контейнера с фильтром по алфавиту
                limit = $('.footer').offset().top - filterContainer.height() - 20;
            },
            error: function () {
            }
        });
    })
})(jQuery);