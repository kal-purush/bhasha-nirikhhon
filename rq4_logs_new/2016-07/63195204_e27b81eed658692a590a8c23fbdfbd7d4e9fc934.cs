using System.Collections.Generic;
using OxyPlot;
using OxyPlot.Series;

namespace HomePlotter
{
    internal class Camembert
    {

        public PlotModel modelP1 { get; set; }

        public string sourceImg { get; set; }

        public Camembert()
        {
            modelP1 = new PlotModel { Title = "Camembert" };
            sourceImg = Program.ImageHouse;
            dynamic seriesP1 = new PieSeries { StrokeThickness = 2.0, InsideLabelPosition = 0.8, AngleSpan = 360, StartAngle = 0 };

            seriesP1.Slices.Add(new PieSlice("no data", 0) { IsExploded = false, Fill = OxyColors.PaleVioletRed });

            modelP1.Series.Add(seriesP1);

        }

        public Camembert(Dictionary<string,double> datasEnumerable)
        {
            modelP1 = new PlotModel { Title = "Camembert" };
            sourceImg = Program.ImageHouse;
            dynamic seriesP1 = new PieSeries { StrokeThickness = 2.0, InsideLabelPosition = 0.8, AngleSpan = 360, StartAngle = 0 };

            foreach (var room in datasEnumerable.Keys)
            {
                seriesP1.Slices.Add(new PieSlice(room, datasEnumerable[room]) { IsExploded = true });
            }

            modelP1.Series.Add(seriesP1);

        }



    }
}