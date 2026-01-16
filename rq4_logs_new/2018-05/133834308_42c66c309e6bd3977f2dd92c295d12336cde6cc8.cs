using DevExpress.Pdf;
using System.Collections.Generic;

namespace RemoveAllMarkupAnnotations {

    class Program {
        static void Main(string[] args) {

            // Create a PDF Document Processor.
            using (PdfDocumentProcessor processor = new PdfDocumentProcessor()) {

                // Load a document.
                processor.LoadDocument("..\\..\\Document.pdf");

                // Get a list of annotations in the document pages.
                for (int i = 0; i <= processor.Document.Pages.Count; i++) {
                    IList<PdfMarkupAnnotationData> annotations = processor.GetMarkupAnnotationData(i);

                    // Delete all text markup annotations from document pages.
                    processor.DeleteMarkupAnnotations(annotations);

                    // Save the result document.
                    processor.SaveDocument("..\\..\\Result.pdf");
                }
            }
        }
    }
}