class histchart {
  constructor (id = 'chart1', title = 'Histogram', data = [1, 5, 6]) {
    this.id = id;
    this.title = title;
    this.data = data;
  }
    chart() {
      return Highcharts.chart(this.id, {
        title: {
          text: 'Highcharts Histogram'
        },
        xAxis: [{
          title: {
            text: this.title
          },
          min: -15,
          max: 15,
          alignTicks: false,
          opposite: false
        }],

        yAxis: [{
          title: {
            text: 'Histogram'
          },
          opposite: false
        }],

        series: [{
          name: 'Histogram',
          type: 'histogram',
          data: this.data,
          id: 's1',
          xAxis: 0,
          yAxis: 0,
          baseSeries: 's1',
          zIndex: -1
        }]
      })
    }
  };