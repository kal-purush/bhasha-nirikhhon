var monthlyRequest = document.getElementById("monthlyRequest").getContext("2d");
var myChart = new Chart(monthlyRequest, {
  type: "line",
  data: {
    labels: ["Geo Dept.", "CED", "Pharmacy", "Chem", "others", "Orange"],
    datasets: [
      {
        label: "# of Votes",
        data: [12, 19, 3, 5, 2, 3],
        backgroundColor: [
          "rgba(255, 99, 132, 0.2)",
          "rgba(54, 162, 235, 0.2)",
          "rgba(255, 206, 86, 0.2)",
          "rgba(75, 192, 192, 0.2)",
          "rgba(153, 102, 255, 0.2)",
          "rgba(255, 159, 64, 0.2)"
        ],
        borderColor: [
          "rgba(255, 99, 132, 1)",
          "rgba(54, 162, 235, 1)",
          "rgba(255, 206, 86, 1)",
          "rgba(75, 192, 192, 1)",
          "rgba(153, 102, 255, 1)",
          "rgba(255, 159, 64, 1)"
        ],
        borderWidth: 1
      }
    ]
  },
  options: {
    scales: {
      yAxes: [
        {
          stacked: true
        }
      ]
    }
  }
});

// And for a doughnut chart for weekly request
var departmentRequest = document
  .getElementById("departmentRequest")
  .getContext("2d");
var myChart = new Chart(departmentRequest, {
  type: "polarArea",
  data: {
    labels: ["Geo Dept.", "CED", "Pharmacy", "Chem", "others"],
    datasets: [
      {
        label: "# of Votes",
        data: [12, 19, 3, 5, 2],
        backgroundColor: [
          "rgba(255, 99, 132, 0.2)",
          "rgba(54, 162, 235, 0.2)",
          "rgba(255, 206, 86, 0.2)",
          "rgba(75, 192, 192, 0.2)",
          "rgba(153, 102, 255, 0.2)"
        ],
        borderColor: [
          "rgba(255, 99, 132, 1)",
          "rgba(54, 162, 235, 1)",
          "rgba(255, 206, 86, 1)",
          "rgba(75, 192, 192, 1)",
          "rgba(153, 102, 255, 1)"
        ],
        borderWidth: 1
      }
    ]
  }
});

// And for a doughnut chart
var mostReqChem = document.getElementById("mostReqChem").getContext("2d");
var myChart = new Chart(mostReqChem, {
  type: "doughnut",
  data: {
    labels: ["Red", "Blue", "Yellow"],
    datasets: [
      {
        label: "# of Votes",
        data: [12, 19, 3],
        backgroundColor: [
          "rgba(255, 205, 86, 0.5)",
          "rgba(54, 162, 235, 0.5)",
          "rgba(255, 99, 132, 0.5)"
        ],
        borderColor: [
          "rgba(255, 205, 86, 1)",
          "rgba(54, 162, 235, 1)",
          "rgba(255, 99, 132, 1)"
        ],
        borderWidth: 1
      }
    ]
  }
});

$(document).on("click", ".graphDetails", function(e) {
  e.preventDefault();
  var status = $(this).attr("data-status");
  if (!status) {
    $(this).attr("data-status", "open");
    $(".dataGraph").show();
  } else {
    $(this).attr("data-status", "");
    $(".dataGraph").hide();
  }
});