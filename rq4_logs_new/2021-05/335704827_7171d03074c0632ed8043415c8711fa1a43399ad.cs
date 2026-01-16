using System;
using System.Collections.Generic;
using System.Drawing;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace OBSSwitcher
{
    public class PainterObject
    {
        public Color RectangleColor;
        public Tuple<int, int> StartRectangleSizes;
        public Tuple<int, int> StopRectangleSizes;

        public Graphics GraphicSet;
        public IntPtr DesktopPointer;
    }

    public class BoundingBoxDisplayHelper
    {
        // Painter Helpers
        [DllImport("User32.dll")]
        public static extern IntPtr GetDC(IntPtr hwnd);
        [DllImport("User32.dll")]
        public static extern void ReleaseDC(IntPtr hwnd, IntPtr dc);

        // Store a list of currently active graphics objects.
        private List<PainterObject> ActiveGraphicsList = new List<PainterObject>();

        /// <summary>
        /// Draws all pane sizes based on a pane size object passed in.
        /// </summary>
        /// <param name="Sizes">Pane Sizes object.</param>
        public void DrawAllRectangles(PaneSizeValues Sizes)
        {
            DrawRectangles(Color.Red, Sizes.StartScreenSizes, Sizes.PaneOneValues);
            DrawRectangles(Color.Green, Sizes.PaneTwoValues, Sizes.PaneThreeValues);
            DrawRectangles(Color.Blue, Sizes.PaneThreeValues, Sizes.MaxScreenSizes);
        }
        /// <summary>
        /// Draws the bound boxes for obs regions. 
        /// </summary>
        /// <param name="PaneNumber">Pane number to draw. If a number is passed in as an invalid value, it shows all panes.</param>
        public void DrawRectangles(Color BrushColor, Tuple<int, int> StartPaneValues, Tuple<int,int> StopPaneValues)
        {
            // Get desktop pointer and the graphics object involved.
            IntPtr DesktopPointer = GetDC(IntPtr.Zero);
            Graphics DesktopGraphic = Graphics.FromHdc(DesktopPointer);

            // Draw the rectangle here.
            Brush NextBrush = new SolidBrush(BrushColor);
            DesktopGraphic.FillRectangle(NextBrush, new Rectangle(
                StartPaneValues.Item1, StartPaneValues.Item2,
                StopPaneValues.Item2, System.Windows.Forms.Screen.PrimaryScreen.Bounds.Height));

            // Make Painter Object.
            var ThisPainter = new PainterObject()
            {
                GraphicSet = DesktopGraphic,
                RectangleColor = BrushColor,
                StartRectangleSizes = StartPaneValues,
                StopRectangleSizes = StopPaneValues,
                DesktopPointer = DesktopPointer
            };

            // Add to list.
            ActiveGraphicsList.Add(ThisPainter);
        }


        /// <summary>
        /// Closes the rectangle which matches the color specified.
        /// </summary>
        /// <param name="CloseColor">Color of rectangle to close.</param>
        public void ClosePainter(Color CloseColor)
        {
            // Get painter object.
            var Painter = ActiveGraphicsList.FirstOrDefault(PaintObj => PaintObj.RectangleColor == CloseColor);
            if (Painter == null) { return; }

            // Close the rectangle.
            Painter.GraphicSet.Dispose();
            ReleaseDC(IntPtr.Zero, Painter.DesktopPointer);
        }
        /// <summary>
        /// Closes all rectangle painters on the system.
        /// </summary>
        public void CloseAllPainters()
        {
            foreach (var Painter in ActiveGraphicsList)
            {
                // Close the rectangle.
                Painter.GraphicSet.Dispose();
                ReleaseDC(IntPtr.Zero, Painter.DesktopPointer);
            }
        }
    }
}