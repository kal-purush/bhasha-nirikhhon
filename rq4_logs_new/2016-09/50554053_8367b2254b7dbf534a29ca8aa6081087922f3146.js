google.charts.load('current', {packages: ['line']});
google.charts.setOnLoadCallback(drawChart);

function drawChart() {
  var data = new google.visualization.DataTable();
  data.addColumn('number', 'X');
  data.addColumn('number', 'added');
  data.addColumn('number', 'removed');

  data.addRows([
    [0, 5, 2],
    [1, 2, 4],
    [3, 4, 5]
  ]);

  var options = {
    chart: {
      title: 'Reading Burndown Chart',
      subtitle: 'Sometimes I buy too many books'
    },
    width: 900,
    height: 500
  };

  var chart = new google.charts.Line(document.getElementById('chart_div'));
  chart.draw(data, options);
}