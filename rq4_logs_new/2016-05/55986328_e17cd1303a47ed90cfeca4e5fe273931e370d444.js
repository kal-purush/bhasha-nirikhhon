
      google.load("visualization", "1", {packages:["corechart"]});
      google.setOnLoadCallback(drawChart);
      function drawChart() {
        var data = google.visualization.arrayToDataTable([
          ['Consumo', 'Porciento'],
          ['Cafe',     60],
          ['Cigarro',     30],
          ['Ron',  10]
        ]);
         var options = {
          title: 'Mi consumo del caf�, cigarro y ron',
          is3D: true,};
        var chart = new google.visualization.PieChart(document.getElementById('piechart'));
        chart.draw(data, options);
      }
