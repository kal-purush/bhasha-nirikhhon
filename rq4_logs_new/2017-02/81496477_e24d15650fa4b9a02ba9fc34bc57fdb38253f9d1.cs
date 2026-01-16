using System;
using System.Collections.Generic;
using System.Windows.Forms;

namespace Herring
{
    class ChartView: PictureBox
    {
        Chart chart = new Chart();

        int hoveredBar = 0;

        
        // IN 5 min quants
        int selectionStart = -1;
        int selectionEnd = -1;
        
        List<ActivitySummary> log;

        /// <summary>
        /// Returns only minutes/hours
        /// </summary>
        public DateTime? SelectionStart
        {
            get
            {
                if (selectionStart == -1)
                    return null;

                
                var span = new TimeSpan(0, (selectionStart / 4) * 5, 0);
                var dateTime = new DateTime() + span;

                return dateTime;
            }

        }

        public TimeSpan? SelectionSpan
        {
            get
            {
                if (selectionStart == -1 || selectionEnd == -1)
                    return null;

                var span = new TimeSpan(0, ((selectionEnd - selectionStart)/4) *5, 0);

                return span;
            }

        }

        public int SelectedBar
        {
            get
            {
                return hoveredBar;
            }

            set
            {
                if (hoveredBar == value)
                    return;

                hoveredBar = value;

                RepaintLog();
            }
        }

        public ChartView()
        {
            Init();
        }

        public void UpdateChart(ActivitySummary summary)
        {
            chart.UpdateChart(summary);
        }

        public void RefreshChart(List<ActivitySummary> items)
        {
            log = new List<ActivitySummary>(items);
            RepaintLog();
        }

        private void RepaintLog()
        {
            chart.CreateChart(log, hoveredBar, selectionStart, selectionEnd);
            Image = chart.Bitmap;
        }

        private void Init()
        {
            BorderStyle = BorderStyle.Fixed3D;
        }

        protected override void OnMouseDown(MouseEventArgs e)
        {
            selectionStart = e.X;
            selectionEnd = e.X;

            base.OnMouseDown(e);
        }

        protected override void OnMouseUp(MouseEventArgs e)
        {
            if (Math.Abs(selectionStart - selectionEnd) < 5)
            {
                selectionStart = -1;
                selectionEnd = -1;

                RepaintLog();
            }

            base.OnMouseUp(e);
        }

        protected override void OnMouseMove(MouseEventArgs e)
        {
            if (e.Button == MouseButtons.Left && Math.Abs(selectionStart - e.X) >= 5) // Draging
            {
                selectionStart = Math.Min(selectionStart, e.X);
                selectionEnd =   Math.Max(selectionEnd, e.X);

                hoveredBar = -1;

                RepaintLog();

                return;
            }

            var newSelection = e.X / 4;

            if (newSelection == hoveredBar)
                return;

            hoveredBar = newSelection;

            RepaintLog();


            base.OnMouseMove(e);
        }

        protected override void OnMouseLeave(EventArgs e)
        {
            if (hoveredBar != -1)
            {
                hoveredBar = -1;
                RepaintLog();
            }
            base.OnMouseLeave(e);
        }
    }
}