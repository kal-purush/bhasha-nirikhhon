var id = "id" + Math.random().toString(16).slice(2);
if (document.cookie.indexOf("cookie") >= 0) {
    var cookie = document.cookie.indexOf("cookie");
}else{
    document.cookie = 'cookie=' + id + ';path=/';
}