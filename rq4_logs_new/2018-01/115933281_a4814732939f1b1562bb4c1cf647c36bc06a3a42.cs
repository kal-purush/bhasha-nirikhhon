using System.Windows.Media;

namespace SEApiComposeImages
{
    class ImageGridSettings
    {
        public int Columns { get; set; }
        public int Rows { get; set; }
        public int CellPixelWidth { get; set; }
        public int CellPixelHeight { get; set; }
        public int Gap { get; set; }
        public int CornerRadiusX { get; set; }
        public int CornerRadiusY { get; set; }
        public Color? BackgroundColor { get; set; }
    }
}