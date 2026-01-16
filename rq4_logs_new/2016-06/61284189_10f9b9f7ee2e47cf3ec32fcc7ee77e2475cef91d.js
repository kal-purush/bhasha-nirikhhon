var statusTableSource = $("#status-table").html();
var statusTableTemplate = Handlebars.compile(statusTableSource);

function loadCrosswordData() {
    // var crosswordType = document.getElementById('type-selector').value;
    // var crosswordId =
    var queryString = URI(window.location.href).query();

    if (queryString) {

        var queryParameters = {};
        var vars = queryString.split('&');
        for (var i = 0; i < vars.length; i++) {
            var pair = vars[i].split('=');
            queryParameters[pair[0]] = pair[1]
        }
        console.log(queryParameters, queryParameters['id'], queryParameters['type']);
        document.getElementById('id-input').value = queryParameters['id'];
        document.getElementById('cword-type').value = queryParameters['type'];


        console.log(getCrosswordStatus(queryParameters['id'], queryParameters['type']))

    }

}


function getCrosswordStatus(id, type) {
    return reqwest({
        url: 'https://sjv0iujqrj.execute-api.eu-west-1.amazonaws.com/PROD/get-status',
        method: 'get',
        crossOrigin: true,
        headers: {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        },
        data: [ { name: 'type', value: type }, { name: 'id', value: id } ]
    }, function(resp) {
        console.log(resp);
        $('body').append(statusTableTemplate({status: resp}));
    })
}

$(document).ready(loadCrosswordData())