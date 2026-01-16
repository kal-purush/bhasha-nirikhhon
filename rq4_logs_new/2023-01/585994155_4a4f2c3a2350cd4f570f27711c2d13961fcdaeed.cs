using PDFBattleBoard.Model;
using PdfSharpCore.Drawing;

namespace PDFBattleBoard.View
{
    internal class MagicDrawer
    {
        public static int CalculateMagicHeight()
        {
            return 20 + 50 + 30 + 30 + 10;
        }

        public static void DrawMagic(MagicDetails characterDetails, XGraphics graphics, XRect containingRectangle)
        {
            DrawHeader(characterDetails, graphics, containingRectangle);
            var currentPoint = containingRectangle.TopLeft;
            currentPoint.Offset(0, 20);
            for (int i = 1; i <= 5; ++i)
            {
                var containingRect = new XRect() { Width = containingRectangle.Width, Height = 10, Location = currentPoint };
                DrawLevel(characterDetails.SpellSlots.First(x => x.Level == i), containingRect, graphics);
                currentPoint.Offset(0, 10);
            }
            
            XPen linePen = new XPen(XBrushes.Black);
            // Couple of blank lines for extra level 5s.
            for (int i = 0; i < 30; i += 10)
            {
                XPoint from = new XPoint() { X = currentPoint.X, Y = currentPoint.Y + i };
                XPoint to = new XPoint() { X = currentPoint.X + containingRectangle.Width, Y = currentPoint.Y + i };
                graphics.DrawLine(linePen, from, to);
            }
            currentPoint.Offset(0, 30);

            for (int i = 6; i <= 8; ++i)
            {
                var containingRect = new XRect() { Width = containingRectangle.Width, Height = 10, Location = currentPoint };
                DrawLevel(characterDetails.SpellSlots.First(x => x.Level == i), containingRect, graphics);
                currentPoint.Offset(0, 10);
            }
            
            var nineRect = new XRect() { Width = containingRectangle.Width / 2, Height = 10, Location = currentPoint };
            DrawLevel(characterDetails.SpellSlots.First(x => x.Level == 9), nineRect, graphics);
            currentPoint.Offset(nineRect.Width, 0);
            
            var tenRect = new XRect() { Width = containingRectangle.Width / 2, Height = 10, Location = currentPoint };
            DrawLevel(characterDetails.SpellSlots.First(x => x.Level == 10), tenRect , graphics);
            currentPoint.Offset(nineRect.Width, 0);

        }

        private static void DrawLevel(SpellSlots spellSlots, XRect containingRectangle, XGraphics graphics)
        {
            double width = containingRectangle.Width;
            var headerRect = new XRect() { Width = width, Height = 10, Location = containingRectangle.TopLeft };
            XFont font = new XFont("Verdana", 9);//, XFontStyle.Bold);
            XPen linePen = new XPen(XBrushes.Black);
            graphics.DrawRectangle(linePen, headerRect);
            
            var currentPoint = containingRectangle.TopLeft;

            var levelRect = new XRect() { Height = 10, Width = 20, Location = currentPoint};
            graphics.DrawRectangle(linePen, levelRect);
            graphics.DrawString(spellSlots.Level.ToString(), font, XBrushes.Black, levelRect, XStringFormats.Center);
            currentPoint.Offset(levelRect.Width, 0);

            var numberRect = new XRect() { Height = 10, Width = 25, Location = currentPoint};
            graphics.DrawRectangle(linePen, numberRect);
            graphics.DrawString(spellSlots.Total.ToString(), font, XBrushes.Black, numberRect, XStringFormats.Center);
            currentPoint.Offset(numberRect.Width, 0);

            var outRect = new XRect() { Height = 10, Width = 20, Location = currentPoint };
            graphics.DrawRectangle(linePen, outRect);
            graphics.DrawString(spellSlots.Out.ToString(), font, XBrushes.Black, outRect, XStringFormats.Center);
        }

        private static void DrawHeader(MagicDetails characterDetails, XGraphics graphics, XRect containingRectangle)
        {
            XPoint currentPoint = containingRectangle.TopLeft;

            double width = containingRectangle.Width;
            var headerRect = new XRect() { Width = width, Height = 10, Location = currentPoint };
            XFont headerFont = new XFont("Verdana", 10, XFontStyle.Bold);
            XPen linePen = new XPen(XBrushes.Black);
            graphics.DrawRectangle(linePen, containingRectangle);
            graphics.DrawRectangle(linePen, headerRect);
            graphics.DrawString("Magic", headerFont, XBrushes.Black, headerRect, XStringFormats.Center);
            currentPoint.Offset(0, 10);
            XFont font = new XFont("Verdana", 10);

            #region Header

            var levelRect = new XRect() { Height = 10, Width = 20, Location = currentPoint };
            graphics.DrawRectangle(linePen, levelRect);
            graphics.DrawString("lvl", font, XBrushes.Black, levelRect, XStringFormats.Center);
            currentPoint.Offset(levelRect.Width, 0);

            var totalSlotsRect = new XRect() { Height = 10, Width = 25, Location = currentPoint };
            graphics.DrawRectangle(linePen, levelRect);
            graphics.DrawString("Total", font, XBrushes.Black, totalSlotsRect, XStringFormats.Center);
            currentPoint.Offset(totalSlotsRect.Width, 0);

            var outRect = new XRect() { Height = 10, Width = 20, Location = currentPoint };
            graphics.DrawRectangle(linePen, levelRect);
            graphics.DrawString("Out", font, XBrushes.Black, outRect, XStringFormats.Center);
            currentPoint.Offset(outRect.Width, 0);

            // Header outline
            var splitWidth = containingRectangle.Width - (levelRect.Width + totalSlotsRect.Width + outRect.Width);
            
            var headerRectangle = new XRect() { Width = width, Height = 10, Location = currentPoint };

            // Total
            var nameRect = new XRect() { Width = splitWidth / 6, Height = 10, Location = currentPoint };
            graphics.DrawRectangle(linePen, nameRect);
            graphics.DrawString("Total Mana", font, XBrushes.Black, nameRect, XStringFormats.Center);

            // TotalVal
            currentPoint.Offset(splitWidth / 6, 0);
            var totalRect = new XRect() { Width = splitWidth / 6, Height = 10, Location = currentPoint };
            graphics.DrawRectangle(linePen, nameRect);

            int total = CalculateTotalMana(characterDetails);

            graphics.DrawString(total.ToString(), font, XBrushes.Black, totalRect, XStringFormats.Center);

            // Mnemonics
            currentPoint.X += splitWidth / 6;
            var SelfTaliRect = new XRect() { Width = splitWidth / 6, Height = 10, Location = currentPoint };
            graphics.DrawRectangle(linePen, SelfTaliRect);
            graphics.DrawString("Mnemonic", font, XBrushes.Black, SelfTaliRect, XStringFormats.Center);

            currentPoint.X += splitWidth / 6;
            var medCountRect = new XRect() { Width = splitWidth / 6, Height = 10, Location = currentPoint };
            var tempPoint = currentPoint;
            for (int i = 0; i < characterDetails.Mnemonics; ++i)
            {
                XPoint start = new XPoint(tempPoint.X + 2, tempPoint.Y + 2);
                XPoint end = new XPoint(start.X + 6, start.Y + 6);
                XRect containingBox = new XRect(start, end);
                graphics.DrawEllipse(linePen, containingBox);
                tempPoint.X += 10;
            }

            // Regain
            currentPoint.X += splitWidth / 6;
            var medsRect = new XRect() { Width = splitWidth /6, Height = 10, Location = currentPoint };
            graphics.DrawRectangle(linePen, medsRect);
            graphics.DrawString("Regain", font, XBrushes.Black, medsRect, XStringFormats.Center);

            // RegainValue
            currentPoint.Offset(splitWidth/ 6, 0);
            var fooRect = new XRect() { Width = splitWidth / 6, Height = 10, Location = currentPoint };
            graphics.DrawRectangle(linePen, nameRect);
            graphics.DrawString(characterDetails.Regain.ToString(), font, XBrushes.Black, fooRect, XStringFormats.Center);

            #endregion
        }

        private static int CalculateTotalMana(MagicDetails magicDetails)
        {
            int value = 0;
            foreach (var item in magicDetails.SpellSlots)
            {
                value += item.Total * item.Level;
            }
            return value;   
        }
    }
}