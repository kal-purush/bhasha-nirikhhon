


//$('#canvas').empty(); //Limpando grafico

//var idFuncionario = $('#cbx_funcionario').val();//Pega valor do campo cbx_funcionario
//torna grafico responsivo
var options = {
    responsive: true,
};
var data;

$.ajax({
    type: "POST",
    url: "../views/getDadosGrafico.php",
    data: {
        data:
                [
                ]},
    cache: false,
    success: function (result) {
        try {
            result = $.parseJSON(result);
            var datas = result['datas'];
            var votos = result['votos'];
            var nome_criterio = result['nome_criterio'];
            //INFORMAÇÕES DO GRAFICO
            //data responsavel pelo valores dos itens e visual do grafico
            data = {
                //cabeçalhos
                labels: datas,
                datasets: [
                    {
                        fill: false,
                        data: votos,
                        label: nome_criterio,
                        fillColor: "rgba(220,220,220,0.5)",
                        strokeColor: "rgba(220,220,220,1)",
                        pointColor: "rgba(220,220,220,1)",
                        pointStrokeColor: "#fff",
                        pointHighlightFill: "#fff",
                        pointHighlightStroke: "rgba(151,187,205,1)",
                    }
                ]
            };
        } catch (err) {
            $("#erros").append(err.message);
        }
    }, error: function (result) {
        $("#erros").append(result);
    }
});
//Script JavaScript que chama o gráfico
window.onload = function () {

    // ctx busca o elemento canvas através do ID que vai guardar as informações para renderizar o gráfico
    //Aqui também é utilizado o método getContext(“2d”) que informa ao elemento canvas que ali será renderizado gráficos 2d.
    var ctx = document.getElementById("canvas").getContext("2d");

    //LineChart vai receber os valores do objeto Chart gerado com base na data 
    //e options declaradas. Aqui informamos o tipo do gráfico que será criado, neste caso, Line
    var LineChart = new Chart(ctx).Line(data, options);

    legend(document.getElementById("lineLegend"), data);
};