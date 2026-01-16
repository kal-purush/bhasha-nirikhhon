jQuery(document).ready(function ($) {
    $('#load2').hide();
    
    function createBookListItem(book) {
        var $li = $('<li>');    
        $li.addClass('list-group-item hover-invert cursor-pointer');
        $li.html(book.title);     
        $li.data('bookId', book.id);
        return $li;
    }

    var request = axios.get('http://csc225.mockable.io/books');
    request.then(function (response) {
        $('#load').hide();
        response.data.forEach(function (book) {
            $('#book-list').prepend(createBookListItem(book));
        });
         
        $('.list-group-item').on('click', function () {
            $('.card').hide();
            $('#load2').show();
            $('#add-card').append('<div>').addClass('card').append('<div>');
            $('.list-group-item').removeClass('active');

            var bookId = $(this).data('bookId');
            $(this).addClass('active');
        
            axios.get('http://csc225.mockable.io/books/' + bookId).then(function (response) {
                $('.card').empty();
                $('.card').show();
                $('#load2').hide();
            
                var $cover = $('<img>').attr('src', response.data.cover).attr('alt', response.data.title).addClass('mt-3');
                var $title = $('<p>').html('Title: ' + response.data.title).addClass('mt-4');
                var $author = $('<p>').html('Author: ' + response.data.author);
                var $pages = $('<p>').html('Pages: ' + response.data.pages);
                var $year = $('<p>').html('Year: ' + response.data.year);
                var $country = $('<p>').html('Country: ' + response.data.country);
                var $language = $('<p>').html('Language: ' + response.data.language);
                var $link = $('<a>').attr('href', response.data.link).html(response.data.link).addClass('mb-2');
                    
                $('.card').append($cover, $title, $author, $pages, $year, $country, $language, $link);
            }); 
        });
    }); 
});