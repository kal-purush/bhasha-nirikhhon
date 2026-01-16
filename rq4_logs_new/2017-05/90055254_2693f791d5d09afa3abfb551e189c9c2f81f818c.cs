using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PDFConverter
{
    class PDFConverter
    {
        public class docxConverter
        {
            public void Convert(string pdfLocation, string wordLocation)
            {
                // Convert PDF file to DOCX file 
                SautinSoft.PdfFocus f = new SautinSoft.PdfFocus();

                f.OpenPdf(pdfLocation);

                if (f.PageCount > 0)
                {
                    // You may choose output format between Docx and Rtf. 
                    f.WordOptions.Format = SautinSoft.PdfFocus.CWordOptions.eWordDocument.Docx;

                    int result = f.ToWord(wordLocation);

                    // Show the resulting Word document. 
                    try
                    {
                        if (result == 0)
                        {
                            System.Diagnostics.Process.Start(wordLocation);
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message, ex.InnerException);
                    }
                }
            }
        }
}