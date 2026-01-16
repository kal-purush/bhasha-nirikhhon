using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ClockWinForms
{
    public class InitialValues
    {
        public InitialValues(float width, float height)
        {
            x0 = width / 2;
            y0 = height / 2;

            CreateCoordinatesOfPoints();

            
        }

        /// <summary>
        /// coordinates of center of the clock face
        /// </summary>
        private float x0;
        private float y0;

        /// <summary>
        /// coordinates of points
        /// </summary>
        private float[] xPoints;
        private float[] yPoints;
        private float radiusPoints;

        /// <summary>
        /// coordinates of sec hand
        /// </summary>
        private float[] x2Sec;
        private float[] y2Sec;
        private float lengthOfSec;

        /// <summary>
        /// coordinates of minute
        /// </summary>
        private float[] x2Minute;
        private float[] y2Minute;
        private float lengthOfMinute;

        /// <summary>
        /// coordinates of points
        /// </summary>
        private float[] x2Hour;
        private float[] y2Hour;
        private float lengthOfHour;

        private void CreateCoordinatesOfPoints()
        {
            xPoints = new float[12];
            yPoints = new float[12];
            int fi = 0;
            xPoints[0] = x0 - 5;
            yPoints[0] = y0 / 10 - 5;
            radiusPoints = y0 - yPoints[0];
            for (int i = 0; i < 12; ++i)
            {
                fi += 30;
                xPoints[i] = x0 + radiusPoints * (float)Math.Cos(((Math.PI * fi) / 180)) - 5;
                yPoints[i] = y0 + radiusPoints * (float)Math.Sin(((Math.PI * fi) / 180)) - 5;
            }
        }

        private void CreateCoordinatesOfHands()
        {
            xPoints = new float[12];
            yPoints = new float[12];
            int fi = 0;
            xPoints[0] = x0 - 5;
            yPoints[0] = y0 / 10 - 5;
            radiusPoints = y0 - yPoints[0];
            for (int i = 0; i < 12; ++i)
            {
                fi += 30;
                xPoints[i] = x0 + radiusPoints * (float)Math.Cos(((Math.PI * fi) / 180)) - 5;
                yPoints[i] = y0 + radiusPoints * (float)Math.Sin(((Math.PI * fi) / 180)) - 5;
            }
        }

        public float[] GetCoordinatesOfxPoints() => xPoints;

        public float[] GetCoordinatesOfyPoints() => yPoints;
    }
}