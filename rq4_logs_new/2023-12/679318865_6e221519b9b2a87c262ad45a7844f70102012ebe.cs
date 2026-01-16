using iText.Html2pdf;
using iText.Kernel.Pdf;
using Microsoft.Extensions.Hosting;

namespace MRF.Utility
{
    public class HTMLtoPDF : IHTMLtoPDF
    {  
        private readonly IHostEnvironment _hostEnvironment;
        public HTMLtoPDF(IHostEnvironment hostEnvironment)
        {
            _hostEnvironment = hostEnvironment;
        }
        public  void CovertHtmlToPDF(string inputHtml, string pdfFileName)
        {
            string pdfDirectory = Path.Combine(_hostEnvironment.ContentRootPath, "PDFs");            
            string pathToOutputPdfFile = Path.Combine(pdfDirectory, pdfFileName);
            if (!Directory.Exists(pdfDirectory))
            {
                Directory.CreateDirectory(pdfDirectory);
            }
            ConvertHtmlToPdf(inputHtml, pathToOutputPdfFile);
        }

        static void ConvertHtmlToPdf(string htmlString, string outputFile)
        {
            using (var pdfWriter = new PdfWriter(outputFile))
            {
                using (var pdfDocument = new PdfDocument(pdfWriter))
                {
                    var converterProperties = new ConverterProperties();
                    HtmlConverter.ConvertToPdf(htmlString, pdfDocument, converterProperties);
                }
            }
        }
    }
}