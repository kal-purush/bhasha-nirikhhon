using System;
using System.Windows.Forms;
using System.Windows.Forms.DataVisualization.Charting;

namespace FHN_nonlocal_coupling.Controller
{
    class ViewElements
    {
        public Chart chart;
        public Chart chartPhase;
        public PropertyGrid pg1, pg2;
        public ProgressBar progressBar;
        public TrackBar trackBar;

        public ViewElements(Chart chart, Chart chartPhase, PropertyGrid pg1, PropertyGrid pg2, ProgressBar progressBar, TrackBar trackBar)
        {
            this.chart = chart;
            this.chartPhase = chartPhase;
            this.pg1 = pg1;
            this.pg2 = pg2;
            this.progressBar = progressBar;
            this.trackBar = trackBar;
        }
    }
}