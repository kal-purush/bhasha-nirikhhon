using System;
using System.Collections.Generic;
using System.Drawing;
using System.Linq;
using System.Text;

namespace LightPanels
{
    static class ColorExtention
    {
        private static Random rdm;
        public static Color Random()
        {
            if (rdm == null)
            {
                rdm = new Random();
            }
            return Color.FromArgb(rdm.Next(0, 254), rdm.Next(0, 254), rdm.Next(0, 254));
        }
    }
}