import Chart from 'chart.js/auto';

const mostQuantity = (loanList) => {
  const chartData = [...loanList].slice(0, 5);

  const chart = new Chart(document.getElementById('chart-quantity'), {
    type: 'bar',
    data: {
      labels: chartData.map((elem) => elem.itemName),
      datasets: [
        {
          label: 'Stock Quantity',
          backgroundColor: [
            '#515164bb', '#8888a5bb',
          ],
          maxBarThickness: 80,
          data: chartData.map((elem) => elem.quantity),
        },
      ],
    },
    options: {
      responsive: false,
      scales: {
        x: {
          ticks: {
            display: false,
          },
        },
        y: {
          beginAtZero: true,
          stepSize: 1,
        },
      },
    },
  });

  return chart;
}

const mostPrice = (loanList) => {
  const chartData = loanList.slice(0, 5);

  const chart = new Chart(document.getElementById('chart-price'), {
    type: 'bar',
    data: {
      labels: chartData.map((elem) => elem.itemName),
      datasets: [
        {
          label: 'Unit Price',
          backgroundColor: [
            '#515164bb', '#8888a5bb',
          ],
          maxBarThickness: 80,
          data: chartData.map((elem) => elem.price),
        },
      ],
    },
    options: {
      responsive: false,
      scales: {
        x: {
          ticks: {
            display: false,
          },
        },
      },
      y: {
        beginAtZero: true,
        stepSize: 1,
      },
    },
  });

  return chart;
}

export default {
  mostQuantity, mostPrice,
}