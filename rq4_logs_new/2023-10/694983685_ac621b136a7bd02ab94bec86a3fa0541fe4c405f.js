/* Atualização automática do strip-match */
var timeOutId = 0;
var requestMatchupESPN = function () {
    $.ajax({
        type: "POST",
        url: "/update_match_strip",
        data: {
            current_week: $('#week').val(),
            weekday: $('#weekday').val(),
            current_year: $('#year').val(),
            hour: $('#hour').val(),
        },
        success: function (response) {
            $('div#strip-match').empty().append(response.data)
            /* Chamar a função 30 s após o término da chamada atual */
            timeOutId = setTimeout(requestMatchupESPN, 30000);
        },
        error: function (response) {
            timeOutId = setTimeout(requestMatchupESPN, 30000);
        }
    });
}
/* Chama pela primeira vez assim que a página é carregada */
requestMatchupESPN();

/* Tabs */
function changeTabs(tab) {
    $.ajax({
        type: "POST",
        url: tab,
        data: {
        },
        success: function (response) {
            $('div#main-content').empty().append(response.data)
        },
        error: function (response) {
            //$('div#main-content').empty().append(response.data) // TODO Colocar HTML com msg de erro
        }
    });
}

function changeStandingsView(tab, year) {
    document.getElementById('season_dropdown').value = year
    $.ajax({
        type: "POST",
        url: "/classificacao",
        data: {
            tab: tab,
            year: year,
        },
        success: function (response) {
            $('div#main-content').empty().append(response.data)
        },
        error: function (response) {
            //$('div#main-content').empty().append(response.data) // TODO Colocar HTML com msg de erro
        }
    });
}