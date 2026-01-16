using PDFBattleBoard.Model;
using PdfSharpCore.Drawing;

namespace PDFBattleBoard.View
{
    internal class ArmourDrawer
    {
        public UtilDrawer UtilDrawer { get; }

        public double ArmourBoxHeight { get; set; } = 30;

        public ArmourDrawer(UtilDrawer utilDrawer)
        {
            UtilDrawer = utilDrawer;
        }

        public void DrawArmour(Armour characterArmour, XRect containingRectangle)
        {
            var subRect = UtilDrawer.CreateRegion(containingRectangle, "Armour");
            var currentPoint = subRect.TopLeft;
            var numberOfArmourValues = characterArmour.Values.Count();

            double itemWidth = subRect.Width / numberOfArmourValues;

            foreach (var item in characterArmour.Values)
            {
                UtilDrawer.TextRectangle(item.Name, itemWidth, currentPoint);
                currentPoint.Offset(0, UtilDrawer.DefaultLineHeight);

                UtilDrawer.TextRectangle(item.Value.ToString(), itemWidth, currentPoint);
                currentPoint.Offset(itemWidth, -UtilDrawer.DefaultLineHeight);
            }

            var mainValueWidth = subRect.Width / 3;

            currentPoint = subRect.TopLeft;
            currentPoint.Offset(0, UtilDrawer.DefaultLineHeight * 2);

            UtilDrawer.TextRectangle("Physical", mainValueWidth, currentPoint);
            currentPoint.Offset(mainValueWidth, 0);
            UtilDrawer.TextRectangle("Power", mainValueWidth, currentPoint);
            currentPoint.Offset(mainValueWidth, 0);
            UtilDrawer.TextRectangle("Magic", mainValueWidth, currentPoint);

            currentPoint = subRect.TopLeft;
            currentPoint.Offset(0, UtilDrawer.DefaultLineHeight * 3);

            UtilDrawer.EmptyBox(mainValueWidth, ArmourBoxHeight, currentPoint);
            currentPoint.Offset(mainValueWidth, 0);
            UtilDrawer.EmptyBox(mainValueWidth, ArmourBoxHeight, currentPoint);
            currentPoint.Offset(mainValueWidth, 0);
            UtilDrawer.EmptyBox(mainValueWidth, ArmourBoxHeight, currentPoint);
        }

        public double CalculateArmourHeight()
        {
            return 2 * UtilDrawer.RegionBuffer + UtilDrawer.DefaultLineHeight * 4 + ArmourBoxHeight;
        }
    }
}