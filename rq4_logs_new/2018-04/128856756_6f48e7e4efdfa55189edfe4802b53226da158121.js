$('#sortAscending').on('click', function () {
    var $photos = $('#photos'),
        $photoArray = $photos.children('#pictureFrame');
        
    $photoArray.sort(function(a,b){
        var an = a.getAttribute('title'),
            bn = b.getAttribute('title');
        if(an > bn) {
            return 1;
        }
        if(an < bn) {
            return -1;
        }
        return 0;
    });

    $photoArray.detach().appendTo($photos);
})

$('#sortDescending').on('click', function () {
    var $photos = $('#photos'),
        $photoArray = $photos.children('#pictureFrame');

    $photoArray.sort(function(a,b){
        var an = a.getAttribute('title'),
            bn = b.getAttribute('title');

        if(an < bn) {
            return 1;
        }
        if(an > bn) {
            return -1;
        }
        return 0;
    });

    $photoArray.detach().appendTo($photos);
})