window.onload = function(){
  var ctx = document.getElementById("Graph").getContext("2d");
  var LineChart = new Chart(ctx).Line(graph, options);
}

var options = {
  responsive:true
};

var graph = {
  labels: ["0", "2", "4", "6", "8", "10"], // 6
  datasets: [
    {
      label: "zwaartekracht",
      labelColor: "white",
      fillColor: "rgba(151,187,200,0.4)",
      strokeColor: "#0059b3",
      pointColor: "#0059b3",
      pointStrokeColor: "#fff",
      pointHighlightFill: "#fff",
      pointHighlightStroke: "#0059b3",
      data: [10000, 0, 1.500]  // 3
    }
  ]
};

//https://codepen.io/vagnerld/pen/KwERem//