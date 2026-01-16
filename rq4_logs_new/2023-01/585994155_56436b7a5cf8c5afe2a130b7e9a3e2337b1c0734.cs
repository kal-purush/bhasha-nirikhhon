using PDFBattleBoard.Model;
using PDFBattleBoard.View;
using PdfSharpCore.Drawing;
using PdfSharpCore.Pdf;
using System.Diagnostics;

namespace PDFBattleBoard
{
    internal class PDFWriter
    {

        static public void WritePdf()
        {
            //Create PDF Document
            PdfDocument document = new PdfDocument();
            //You will have to add Page in PDF Document
            PdfPage page = document.AddPage();

            page.Orientation = PdfSharpCore.PageOrientation.Landscape;

            //For drawing in PDF Page you will nedd XGraphics Object
            XGraphics gfx = XGraphics.FromPdfPage(page);
            ////For Test you will have to define font to be used

            var powerPool = new PoolAbility()
            {
                Name = "Power",
                Total = 200,
                Talisman = 50,
                Self = 150,
                MedCharges = 2
            };

            var Kai = new PoolAbility()
            {
                Name = "Kai",
                Total = 200,
                Talisman = 50,
                Self = 150,
                MedCharges = 2
            };

            var Psy = new PoolAbility()
            {
                Name = "Psy",
                Total = 200,
                Talisman = 50,
                Self = 150,
                MedCharges = 2
            };

            var adrenalPool = new PoolAbility()
            {
                Name = "Adrenals",
                Total = 200,
                Talisman = 50,
                Self = 150,
                MedCharges = 2
            };

            var lifePool = new PoolAbility()
            {
                Name = "LifePool",
                Total = 500,
                Talisman = 0,
                Self = 0,
                MedCharges = 2
            };

            XRect topLeft = new XRect() { Height = page.Height / 3, Width = page.Width / 2, Location = new XPoint() { X = 0, Y = 0 } };
            XRect middleLeft = new XRect() { Height = page.Height / 3, Width = page.Width / 2, Location = new XPoint() { X = 0, Y = page.Height / 3 } };
            XRect bottomLeft = new XRect() { Height = page.Height / 3, Width = page.Width / 2, Location = new XPoint() { X = 0, Y = (page.Height / 3) * 2 } };

            XRect topRight = new XRect() { Height = page.Height / 2, Width = page.Width / 2, Location = new XPoint() { X = page.Width / 2, Y = 0 } };
            XRect bottomRight = new XRect() { Height = page.Height / 2, Width = page.Width / 2, Location = new XPoint() { X = page.Width / 2, Y = page.Height / 2 } };


            PoolDrawer.DrawPool(powerPool, gfx, topLeft);
            PoolDrawer.DrawPool(lifePool, gfx, middleLeft);
            PoolDrawer.DrawPool(Psy, gfx, bottomLeft);


            ChargedAbility ab1 = new ChargedAbility()
            {
                Name = "Resist Knockback",
                Charges = 5,
                Frequent = Frequency.Daily
            };

            ChargedAbility ab2 = new ChargedAbility()
            {
                Name = "Resist Disarm",
                Charges = 4,
                Frequent = Frequency.Sectional
            };

            ChargedAbility ab3 = new ChargedAbility()
            {
                Name = "Resist Dismember",
                Charges = 3,
                Frequent = Frequency.PerEvent
            };

            XRect abilityOne = new XRect { Height = 10, Width = page.Width / 2, Location = new XPoint() { X = page.Width / 2, Y = 0 } };
            ChargedAbilityDrawer.DrawChargedAbility(ab1, gfx, abilityOne);
            
            XRect abilityTwo = new XRect { Height = 10, Width = page.Width / 2, Location = new XPoint() { X = page.Width / 2, Y = 10 } };
            ChargedAbilityDrawer.DrawChargedAbility(ab2, gfx, abilityTwo);
            
            XRect abilityThree = new XRect { Height = 10, Width = page.Width / 2, Location = new XPoint() { X = page.Width / 2, Y = 20 } };
            ChargedAbilityDrawer.DrawChargedAbility(ab3, gfx, abilityThree);

            //PoolDrawer.DrawPool(Kai, gfx, topRight);
            //PoolDrawer.DrawPool(adrenalPool, gfx, bottomRight);

            //Specify file name of the PDF file
            string filename = "FirstPDFDocument.pdf";
            //Save PDF File
            document.Save(filename);
        }
    }
}