using System;
using System.Diagnostics.CodeAnalysis;
using System.Drawing;
using System.Windows.Forms;
using Microsoft.Win32;

namespace SysInfoReflection
{
    [SuppressMessage("ReSharper", "VirtualMemberCallInConstructor")]
    public class SysInfoReflection:Form
    {
        // ReSharper disable once InconsistentNaming
        protected int ICount;
        protected string[] AstrLabels;
        protected string[] AstrValues;
        protected float CxCol;
        protected int CySpace;

        public static void Main()
        {
            Application.Run(new SysInfoReflection());
        }

        public SysInfoReflection()
        {
            Text = "System Infomation:Reflection";
            BackColor = SystemColors.Window;
            ForeColor = SystemColors.WindowText;
            AutoScroll = true;

            SystemEvents.UserPreferenceChanged += UserPreferenceChanged;
            SystemEvents.DisplaySettingsChanged += DisplaySettingsChanged;

            UpdateAllInfo();
        }

        void DisplaySettingsChanged(object obj, EventArgs ea)
        {
            UpdateAllInfo();
            Invalidate();
        }

        void UserPreferenceChanged(object obj, UserPreferenceChangedEventArgs ea)
        {
            UpdateAllInfo();
            Invalidate();
        }

        void UpdateAllInfo()
        {
            ICount = SysInfoReflectionStrings.SysInfoReflectionStrings.Count;
            AstrLabels = SysInfoReflectionStrings.SysInfoReflectionStrings.Labels;
            AstrValues = SysInfoReflectionStrings.SysInfoReflectionStrings.Values;

            Graphics grfx = CreateGraphics();
            SizeF sizeF = grfx.MeasureString(" ", Font);
            CxCol = sizeF.Width + SysInfoReflectionStrings.SysInfoReflectionStrings.MaxLabelWidth(grfx, Font);
            CySpace = Font.Height;

            AutoScrollMinSize=new Size((int)Math.Ceiling(CxCol+SysInfoReflectionStrings.SysInfoReflectionStrings.MaxValueWidth(grfx,Font)),
                (int)Math.Ceiling((float)CySpace*ICount));

            grfx.Dispose();
        }

        protected override void OnPaint(PaintEventArgs e)
        {
            Graphics grfx = e.Graphics;
            Brush brush = new SolidBrush(ForeColor);
            Point pt = AutoScrollPosition;

            int iFirst = (e.ClipRectangle.Top - pt.Y) / CySpace;
            int iLast = (e.ClipRectangle.Bottom - pt.Y) / CySpace;
            Console.WriteLine(CySpace);
            Console.WriteLine("Top: "+e.ClipRectangle.Top);
            Console.WriteLine("Height: " + e.ClipRectangle.Height);
            Console.WriteLine("Botton: " + e.ClipRectangle.Bottom);
            Console.WriteLine("Top2:" + ClientRectangle.Top);
            Console.WriteLine("Botton2:" + ClientRectangle.Bottom);
            Console.WriteLine("PT.Y: " + pt.Y);
            Console.WriteLine();
            iLast = Math.Min(ICount - 1, iLast);

            for (int i = iFirst; i <= iLast; i++)
            {
                grfx.DrawString(AstrLabels[i], Font, brush, pt.X, pt.Y + i * CySpace);
                grfx.DrawString(AstrValues[i], Font, brush, pt.X + CxCol, pt.Y + i * CySpace);
            }
        }
    }
}