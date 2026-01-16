using LiveCharts;
using LiveCharts.Wpf;


namespace MyAppModBus.Models {
  public class ModBussSchedule : MainWindow {


    
    SeriesCollection serCol = new SeriesCollection {

    new LineSeries{ Values = new ChartValues<double>{ 3, 5, 7, 4} },
    new ColumnSeries{ Values = new ChartValues<decimal> { 5, 6, 2, 7} }

    };

  }
}