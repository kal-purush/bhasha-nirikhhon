using System;
using System.Diagnostics;
using System.Windows.Media;

namespace SEApiComposeImages
{
    [DebuggerDisplay("H: {Hue}, S: {Saturation}, V: {Value}")]
    class HSVColor
    {
        public double Hue { get; }
        public double Saturation { get; }
        public double Value { get; }

        public HSVColor(float hue, double saturation, double value)
        {
            Hue = hue;
            Saturation = saturation;
            Value = value;
        }

        static public HSVColor FromRgbColor(Color color)
        {
            int max = Math.Max(color.R, Math.Max(color.G, color.B));
            int min = Math.Min(color.R, Math.Min(color.G, color.B));

            var hue = System.Drawing.Color.FromArgb(color.A, color.R, color.G, color.B).GetHue();
            var saturation = (max == 0) ? 0 : 1d - (1d * min / max);
            var value = max / 255d;
            return new HSVColor(hue, saturation, value);
        }

        static public bool AreClose(HSVColor l, HSVColor r)
        {
            var isLGray = l.Saturation < 0.1;
            var isRGray = r.Saturation < 0.1;

            if (isLGray && isRGray)
                return Math.Abs(l.Value - r.Value) <= 0.1;
            else if (isLGray != isRGray)
                return false;
            else
                return Math.Abs(l.Hue - r.Hue) <= 5;
        }
    }
}