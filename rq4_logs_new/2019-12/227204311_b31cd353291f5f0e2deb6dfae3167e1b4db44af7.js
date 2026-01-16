$(document).ready(function() {
    const url = 'https://api.myjson.com/bins/jcmhn';
    $.getJSON(url, function(data){
        $('#result').text(data.text);
    });
    const operators = $('.form-control');
    const dict = {};
    for (i=0; i < operators.length; i++) {
        let key = 'var' + String(i + 1);
        dict[key] = operators[i].value;
    }
    $('#button-fetch').on('click', function() {
        let text = $('#result').text();
        let index = dict['var7'];
        delete dict['var7'];
        dict['speach'] = index;
        for (key in dict) {
            text = text.replace(new RegExp('{' +  key + '}', 'g'), dict[key]);
        }
        $('#result').text(text);
    });
});