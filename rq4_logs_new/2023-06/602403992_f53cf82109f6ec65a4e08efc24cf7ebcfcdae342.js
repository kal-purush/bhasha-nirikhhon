// ambile elemen yang kita butuhkan( content )
var keyword = document.getElementById('keyword');
var search = document.getElementById('search');
var container = document.getElementById('content');

// tambahkan event ketika keyword ditulis
keyword.addEventListener('keyup', function() {

    // buat object ajax
    var xhr = new XMLHttpRequest();

    // cek kesiapan ajax
    xhr.onreadystatechange = function() {
        if( xhr.readyState == 4 && xhr.status == 200){
            container.innerHTML = xhr.responseText;
        }
    }

    // eksekusi ajax
    xhr.open('GET', '../ajax/content.php?keyword=' + keyword.value, true);
    xhr.send();

});