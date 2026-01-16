using DocumentFormat.OpenXml.Packaging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DemoDocumentFormat.OpenXML
{
    class Program
    {
        static void Main(string[] args)
        {
            // Apply the Heading 3 style to a paragraph.   
            string fileName = @"C:\Users\Public\Documents\WordProcessingEx.docx";
            using (WordprocessingDocument myDocument = WordprocessingDocument.Open(fileName, true))
            {
                // Get the first paragraph.  
                Paragraph p = myDocument.MainDocumentPart.Document.Body.Elements<Paragraph>().First();

                // If the paragraph has no ParagraphProperties object, create a new one.  
                if (p.Elements<ParagraphProperties>().Count() == 0)
                    p.PrependChild<ParagraphProperties>(new ParagraphProperties());

                // Get the ParagraphProperties element of the paragraph.  
                ParagraphProperties pPr = p.Elements<ParagraphProperties>().First();

                // Set the value of ParagraphStyleId to "Heading3".  
                pPr.ParagraphStyleId = new ParagraphStyleId() { Val = "Heading3" };
            }
            Console.WriteLine("All done. Press a key.");
            Console.ReadKey();
        }
    }
}