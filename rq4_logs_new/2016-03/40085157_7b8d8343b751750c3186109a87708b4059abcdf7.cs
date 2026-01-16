using System;
using System.Windows.Forms;
using System.Windows.Forms.DataVisualization.Charting;

namespace FHN_nonlocal_coupling
{
    class ODEModel
    {
        private ODE[] odes;

        public void formClosing()
        {
            for (int i = 0; i < odes.Length; i++)
            {
                odes[i].dispose(); 
                odes[i] = null;
            }

            odes = null;
        }

        public void loadEquations(bool chckd, PropertyGrid pg1, PropertyGrid pg2)
        {
            int count;
            if (chckd) count = 2;
            else count = 1;

            odes = new ODE[count];

            for (int i = 0; i < count; i++)
                odes[i] = new ODE();

            pg1.SelectedObject = odes[0];

            if (count == 2)
                pg2.SelectedObject = odes[1];
            else
                pg2.SelectedObject = null;
        }

        /// <summary>
        /// Call to solve equations
        /// <para>Returns -1, if computation error occurred,
        /// 0 otherwise.</para>
        /// </summary>
        public int btnSolveClick(ProgressBar progressBar)
        {
            for (int i = 0; i < odes.Length; i++)
                odes[i].load();
            progressBar.Value++;

            for (int i = 0; i < odes.Length; i++)
                odes[i].initials();
            progressBar.Value++;

            for (int i = 0; i < odes.Length; i++)
                if (odes[i].solve() != 0) return -1;
            progressBar.Value++;

            return 0;
        }

        /// <summary>
        /// Plots single point on t plot and phase plane
        /// </summary>
        private void plot(int j, ODE obj, int numEq, Chart chart, Chart chartPhase)
        {
            double t = obj.getT(j);
            double u = obj.getU(j);
            double v = obj.getV(j);

            chart.Series[2 * numEq].Points.AddXY(t, u);
            chart.Series[2 * numEq + 1].Points.AddXY(t, v);

            chartPhase.Series[3 * numEq].Points.AddXY(u, v);
        }

        /// <summary>
        /// Plots nullclines
        /// </summary>
        private void plotNullclines(ODE obj, int numEq, Chart chartPhase)
        {
            for (int j = 0; j < obj.N; j++)
            {
                double un = obj.getUN(j);

                chartPhase.Series[3 * numEq + 1].Points.AddXY(un, obj.getV1(j));
                chartPhase.Series[3 * numEq + 2].Points.AddXY(un, obj.getV2(j));
            }
        }

        /// <summary>
        /// Plots all 'till the trackBarValue moment
        /// </summary>
        public void plotTrackBarScroll(int trackBarValue, Chart chart, Chart chartPhase)
        {
            for (int j = 0; j < trackBarValue; j++)
                for (int i = 0; i < odes.Length; i++)
                    plot(j, odes[i], i, chart, chartPhase);
        }

        /// <summary>
        /// Plots all at once
        /// </summary>
        public void plotAll(Chart chart, Chart chartPhase)
        {
            for (int i = 0; i < odes.Length; i++)
            {
                plotNullclines(odes[i], i, chartPhase);
                for (int j = 0; j < odes[i].N; j++)
                    plot(j, odes[i], i, chart, chartPhase);
            }
        }

        /// <summary>
        /// Plots single points according to trackBar
        /// </summary>
        public void plotTimerTick(TrackBar trackBar, Chart chart, Chart chartPhase)
        {
            if (trackBar.Value == trackBarMax())
            {
                for (int i = 0; i < odes.Length; i++)
                    plot(trackBar.Value, odes[i], i, chart, chartPhase);

                trackBar.Value = 0;
            }
            else
            {
                trackBar.Value++;
                for (int i = 0; i < odes.Length; i++)
                    plot(trackBar.Value, odes[i], i, chart, chartPhase);
            }
        }


        public double chartXMax()
        {
            return odes[0].T + 1;
        }

        public double chartPhaseXMin()
        {
            return -odes[0].L;
        }

        public double chartPhaseXMax()
        {
            return odes[0].L;
        }

        public double chartPhaseYMax()
        {
            return 1.5 + odes[0].I;
        }

        public int trackBarMax()
        {
            return odes[0].N - 1;
        }
    }
}