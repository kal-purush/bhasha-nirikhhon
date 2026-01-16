using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using System.Diagnostics;

namespace ClockWinForms
{
    public partial class Clock : Form
    {
        public Clock()
        {
            InitializeComponent();
            side = ClockFace.Width > ClockFace.Height ? ClockFace.Height : ClockFace.Width;
            xRetreat = ClockFace.Width > ClockFace.Height ? (ClockFace.Width - ClockFace.Height) / 2 : 0;
            yRetreat = ClockFace.Height > ClockFace.Width ? (ClockFace.Height - ClockFace.Width) / 2 : 0;

            initialValues = new InitialValues(ClockFace.Width, ClockFace.Height);
            x0 = initialValues.x0;
            y0 = initialValues.y0;
            x_ofPoints = initialValues.GetCoordinatesOfxPoints();
            y_ofPoints = initialValues.GetCoordinatesOfyPoints();
            x2_ofSec = initialValues.Get_x2_OfSecHand();
            y2_ofSec = initialValues.Get_y2_OfSecHand();
            x2_ofMinute = initialValues.Get_x2_OfMinuteHand();
            y2_ofMinute = initialValues.Get_y2_OfMinuteHand();
            x2_ofHour = initialValues.Get_x2_OfHourHand();
            y2_ofHour = initialValues.Get_y2_OfHourHand();

            redPen40 = new Pen(Color.Red, side / 10);
            redPen10 = new Pen(Color.Red, side / 40);
            secPen = new Pen(Color.White, side / 80);
            minPen = new Pen(Color.DarkBlue, side / 50);
            hourPen = new Pen(Color.Red, side / 40);
        
            i = DateTime.Now.Second;
            j = DateTime.Now.Minute;
            k = (DateTime.Now.Hour) % 12 * 5 + j / 12;

            Timer timer = new Timer();
            timer.Interval = 1000;
            timer.Tick += PerformTick;
            timer.Enabled = true;
        }

        InitialValues initialValues;

        private int i;
        private int j;
        private int k;

        private float side;
        private float xRetreat;
        private float yRetreat;
        private float x0;
        private float y0;
        private float[] x_ofPoints;
        private float[] y_ofPoints;
        private float[] x2_ofSec;
        private float[] y2_ofSec;
        private float[] x2_ofMinute;
        private float[] y2_ofMinute;
        private float[] x2_ofHour;
        private float[] y2_ofHour;

        private Brush yellow = Brushes.Yellow;
        private Brush red = Brushes.Red;
        private Brush blue = Brushes.Blue;
        private Pen redPen40;
        private Pen redPen10;
        private Pen secPen;
        private Pen minPen;
        private Pen hourPen;

        private void DrawingOfClockFace(object sender, PaintEventArgs e)
        {
            e.Graphics.FillEllipse(yellow, side / 40 * 3 + xRetreat, side / 40 * 3 + yRetreat, side * 17 / 20, side * 17 / 20);
            e.Graphics.FillEllipse(red, side / 80 + xRetreat, side / 80 + yRetreat, side * 3 / 20, side * 3 / 20);
            e.Graphics.FillEllipse(blue, side * 67 / 80 + xRetreat, side * 67 / 80 + yRetreat, side * 3 / 20, side * 3 / 20);

            e.Graphics.DrawEllipse(redPen40, side / 40 * 3 + xRetreat, side / 40 * 3 + yRetreat, side * 17 / 20, side * 17 / 20);
            e.Graphics.DrawEllipse(redPen10, side / 5 * 2 + xRetreat, side / 5 * 2 + yRetreat, side / 5, side / 5);
 
            e.Graphics.DrawLine(secPen, x0, y0, x2_ofSec[i], y2_ofSec[i]);
            e.Graphics.DrawLine(minPen, x0, y0, x2_ofMinute[j], y2_ofMinute[j]);
            e.Graphics.DrawLine(hourPen, x0, y0, x2_ofHour[k], y2_ofHour[k]);

            for (int i = 0; i < 12; ++i)
            {
                e.Graphics.FillEllipse(yellow, x_ofPoints[i], y_ofPoints[i], side / 40, side / 40);
            }

            e.Graphics.FillEllipse(red, side * 19 / 40 + xRetreat, side * 19 / 40 + yRetreat, side / 20, side / 20);
        }

        private void PerformTick(object sender, EventArgs e)
        {   
            k = (DateTime.Now.Hour) % 12 * 5 + j / 12;
            j = DateTime.Now.Minute;
            i = DateTime.Now.Second;
            ClockFace.Invalidate();
        }
    }
}