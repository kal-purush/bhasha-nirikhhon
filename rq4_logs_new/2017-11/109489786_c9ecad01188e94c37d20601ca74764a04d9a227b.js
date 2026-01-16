$(document).ready(function(){
    $('#btn-go').click(function(){
        url = window.location.protocol + '//'+window.location.hostname +':' + window.location.port +'/eval_emotion/'+$('#username').val();
        window.location.assign(url);
    });
});