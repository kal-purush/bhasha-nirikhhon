//Chart Wrapper CCRO Charts


google.charts.load('current');
google.charts.setOnLoadCallback(drawTemp);
google.charts.setOnLoadCallback(drawPH);
google.charts.setOnLoadCallback(drawDO);
google.charts.setOnLoadCallback(drawDOPer);
google.charts.setOnLoadCallback(drawCond);
google.charts.setOnLoadCallback(drawDOC);
google.charts.setOnLoadCallback(drawTDN);
google.charts.setOnLoadCallback(drawNO3);
google.charts.setOnLoadCallback(drawNH4);
google.charts.setOnLoadCallback(drawPO4);
//google.charts.setOnLoadCallback(drawSi);

function drawTemp() {
  var wrapper = new google.visualization.ChartWrapper({
    chartType: 'AnnotationChart',
    dataSourceUrl: 'https://docs.google.com/spreadsheets/d/1vlUP7zE5znhA8jW0ipfGfLT-o0pa0DIt-6JPa73czRQ/edit?usp=sharing',
    query: 'SELECT A, B, C, D, E, F, G',
    options: {
      'chart': {
        'backgroundColor': 'transparent',
        'chartArea': {
          'backgroundColor': 'transparent'
        },
        'seriesType': 'line',
        'curveType': 'function'
      },
      'displayZoomButtons': 'false',
      'thickness': '2',
      'fontSize': '18',
      'range': {
        'ui': {
          'chartOptions': {
            'backgroundColor': 'transparent',
            'height': '50',
            'chartArea': {
              'backgroundColor': 'transparent'
            },
          }
        },
        'table': {
          'backgroundColor': 'transparent'
        }
      }
    },
    containerId: 'temp_div'
  });
  wrapper.draw()
}

function drawPH() {
  var wrapper = new google.visualization.ChartWrapper({
    chartType: 'AnnotationChart',
    dataSourceUrl: 'https://docs.google.com/spreadsheets/d/1tywh1YM_QgbpHZg225BCn6Zqg5-JOseFv9lJ-UYIJUg/edit?usp=sharing',
    query: 'SELECT A, B, C, D, E, F, G',
    options: {
      'chart': {
        'backgroundColor': 'transparent',
        'chartArea': {
          'backgroundColor': 'transparent'
        },
        'seriesType': 'line',
        'curveType': 'function'
      },
      'displayZoomButtons': 'false',
      'thickness': '2',
      'fontSize': '18',
      'range': {
        'ui': {
          'chartOptions': {
            'backgroundColor': 'transparent',
            'height': '50',
            'chartArea': {
              'backgroundColor': 'transparent'
            },
          }
        },
        'table': {
          'backgroundColor': 'transparent'
        }
      }
    },
    containerId: 'ph_div'
  });
  wrapper.draw()
}

function drawDO() {
  var wrapper = new google.visualization.ChartWrapper({
    chartType: 'AnnotationChart',
    dataSourceUrl: 'https://docs.google.com/spreadsheets/d/17NUk1mq8sYSLdjprr3V5j2UIwiaGiKkHTseSox0wQks/edit?usp=sharing',
    query: 'SELECT A, B, C, D, E, F, G',
    options: {
      'chart': {
        'backgroundColor': 'transparent',
        'chartArea': {
          'backgroundColor': 'transparent'
        },
        'seriesType': 'line',
        'curveType': 'function'
      },
      'displayZoomButtons': 'false',
      'thickness': '2',
      'fontSize': '18',
      'range': {
        'ui': {
          'chartOptions': {
            'backgroundColor': 'transparent',
            'height': '50',
            'chartArea': {
              'backgroundColor': 'transparent'
            },
          }
        },
        'table': {
          'backgroundColor': 'transparent'
        }
      }
    },
    containerId: 'do_div'
  });
  wrapper.draw()
}

function drawDOPer() {
  var wrapper = new google.visualization.ChartWrapper({
    chartType: 'AnnotationChart',
    dataSourceUrl: 'https://docs.google.com/spreadsheets/d/1HfhrZdwXkc1y3s2JpB8-IrMf-M5saCiZNkkRIiqPqdo/edit?usp=sharing',
    query: 'SELECT A, B, C, D, E, F, G',
    options: {
      'chart': {
        'backgroundColor': 'transparent',
        'chartArea': {
          'backgroundColor': 'transparent'
        },
        'seriesType': 'line',
        'curveType': 'function'
      },
      'displayZoomButtons': 'false',
      'thickness': '2',
      'fontSize': '18',
      'range': {
        'ui': {
          'chartOptions': {
            'backgroundColor': 'transparent',
            'height': '50',
            'chartArea': {
              'backgroundColor': 'transparent'
            },
          }
        },
        'table': {
          'backgroundColor': 'transparent'
        }
      }
    },
    containerId: 'doper_div'
  });
  wrapper.draw()
}

function drawCond() {
  var wrapper = new google.visualization.ChartWrapper({
    chartType: 'AnnotationChart',
    dataSourceUrl: 'https://docs.google.com/spreadsheets/d/1DQmAzU626T1j80uS4LdEmTrbYgiI-YoVniE8H0c1Lu0/edit?usp=sharing',
    query: 'SELECT A, B, C, D, E, F, G',
    options: {
      'chart': {
        'backgroundColor': 'transparent',
        'chartArea': {
          'backgroundColor': 'transparent'
        },
        'seriesType': 'line',
        'curveType': 'function'
      },
      'displayZoomButtons': 'false',
      'thickness': '2',
      'fontSize': '18',
      'range': {
        'ui': {
          'chartOptions': {
            'backgroundColor': 'transparent',
            'height': '50',
            'chartArea': {
              'backgroundColor': 'transparent'
            },
          }
        },
        'table': {
          'backgroundColor': 'transparent'
        }
      }
    },
    containerId: 'cond_div'
  });
  wrapper.draw()
}

function drawDOC() {
  var wrapper = new google.visualization.ChartWrapper({
    chartType: 'AnnotationChart',
    dataSourceUrl: 'https://docs.google.com/spreadsheets/d/1K8B1b-qk8x08CAP-nXV7lo4iKfIspYUG0wz7ogemp5Q/edit?usp=sharing',
    query: 'SELECT A, B, C, D, E, F, G',
    options: {
      'chart': {
        'backgroundColor': 'transparent',
        'chartArea': {
          'backgroundColor': 'transparent'
        },
        'seriesType': 'line',
        'curveType': 'function'
      },
      'displayZoomButtons': 'false',
      'thickness': '2',
      'fontSize': '18',
      'range': {
        'ui': {
          'chartOptions': {
            'backgroundColor': 'transparent',
            'height': '50',
            'chartArea': {
              'backgroundColor': 'transparent'
            },
          }
        },
        'table': {
          'backgroundColor': 'transparent'
        }
      }
    },
    containerId: 'doc_div'
  });
  wrapper.draw()
}

function drawTDN() {
  var wrapper = new google.visualization.ChartWrapper({
    chartType: 'AnnotationChart',
    dataSourceUrl: 'https://docs.google.com/spreadsheets/d/1mp-h0c1Y_4FhyWX73IGjuBpd3VU8Dn_GbQWerx-Ix9U/edit?usp=sharing',
    query: 'SELECT A, B, C, D, E, F, G',
    options: {
      'chart': {
        'backgroundColor': 'transparent',
        'chartArea': {
          'backgroundColor': 'transparent'
        },
        'seriesType': 'line',
        'curveType': 'function'
      },
      'displayZoomButtons': 'false',
      'thickness': '2',
      'fontSize': '18',
      'range': {
        'ui': {
          'chartOptions': {
            'backgroundColor': 'transparent',
            'height': '50',
            'chartArea': {
              'backgroundColor': 'transparent'
            },
          }
        },
        'table': {
          'backgroundColor': 'transparent'
        }
      }
    },
    containerId: 'tdn_div'
  });
  wrapper.draw()
}

function drawNO3() {
  var wrapper = new google.visualization.ChartWrapper({
    chartType: 'AnnotationChart',
    dataSourceUrl: 'https://docs.google.com/spreadsheets/d/17UHSCwNa6LaGchzpGXUn-KCjeveJcoStHp5cW2yFRx0/edit?usp=sharing',
    query: 'SELECT A, B, C, D, E, F, G',
    options: {
      'chart': {
        'backgroundColor': 'transparent',
        'chartArea': {
          'backgroundColor': 'transparent'
        },
        'seriesType': 'line',
        'curveType': 'function'
      },
      'displayZoomButtons': 'false',
      'thickness': '2',
      'fontSize': '18',
      'range': {
        'ui': {
          'chartOptions': {
            'backgroundColor': 'transparent',
            'height': '50',
            'chartArea': {
              'backgroundColor': 'transparent'
            },
          }
        },
        'table': {
          'backgroundColor': 'transparent'
        }
      }
    },
    containerId: 'no3_div'
  });
  wrapper.draw()
}

function drawNH4() {
  var wrapper = new google.visualization.ChartWrapper({
    chartType: 'AnnotationChart',
    dataSourceUrl: 'https://docs.google.com/spreadsheets/d/1QvWySDRIK6kOzslCKjK_yQCKQZUcPT9Lmg4fikxYK2s/edit?usp=sharing',
    query: 'SELECT A, B, C, D, E, F, G',
    options: {
      'chart': {
        'backgroundColor': 'transparent',
        'chartArea': {
          'backgroundColor': 'transparent'
        },
        'seriesType': 'line',
        'curveType': 'function'
      },
      'displayZoomButtons': 'false',
      'thickness': '2',
      'fontSize': '18',
      'range': {
        'ui': {
          'chartOptions': {
            'backgroundColor': 'transparent',
            'height': '50',
            'chartArea': {
              'backgroundColor': 'transparent'
            },
          }
        },
        'table': {
          'backgroundColor': 'transparent'
        }
      }
    },
    containerId: 'nh4_div'
  });
  wrapper.draw()
}

function drawPO4() {
  var wrapper = new google.visualization.ChartWrapper({
    chartType: 'AnnotationChart',
    dataSourceUrl: 'https://docs.google.com/spreadsheets/d/1IFHtk1aFT_8u4F_UCGOR66USk1ja8hou69sBYjg9gno/edit?usp=sharing',
    query: 'SELECT A, B, C, D, E, F, G',
    options: {
      'chart': {
        'backgroundColor': 'transparent',
        'chartArea': {
          'backgroundColor': 'transparent'
        },
        'seriesType': 'line',
        'curveType': 'function'
      },
      'displayZoomButtons': 'false',
      'thickness': '2',
      'fontSize': '18',
      'range': {
        'ui': {
          'chartOptions': {
            'backgroundColor': 'transparent',
            'height': '50',
            'chartArea': {
              'backgroundColor': 'transparent'
            },
          }
        },
        'table': {
          'backgroundColor': 'transparent'
        }
      }
    },
    containerId: 'po4_div'
  });
  wrapper.draw()
}

function drawSi() {
  var wrapper = new google.visualization.ChartWrapper({
    chartType: 'AnnotationChart',
    dataSourceUrl: 'https://docs.google.com/spreadsheets/d/1A9zvqFUegswsPHtK0-rrfHP7ptxi0965JV7F6nLwsl0/edit?usp=sharing',
    query: 'SELECT A, B, C, D, E, F, G',
    options: {
      'chart': {
        'backgroundColor': 'transparent',
        'chartArea': {
          'backgroundColor': 'transparent'
        },
        'seriesType': 'line',
        'curveType': 'function'
      },
      'displayZoomButtons': 'false',
      'thickness': '2',
      'fontSize': '18',
      'range': {
        'ui': {
          'chartOptions': {
            'backgroundColor': 'transparent',
            'height': '50',
            'chartArea': {
              'backgroundColor': 'transparent'
            },
          }
        },
        'table': {
          'backgroundColor': 'transparent'
        }
      }
    },
    containerId: 'si_div'
  });
  wrapper.draw()
}


$(window).resize(function(){
  drawTemp();
  drawPH();
  drawDO();
  drawDOPer();
  drawCond();
  drawDOC();
  drawTDN();
  drawNO3();
  drawNH4();
  drawPO4();
  //drawSi();
});