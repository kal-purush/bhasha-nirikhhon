//This sample is from 
// https://stackoverflow.com/questions/41385707/how-to-add-page-numbers-in-format-x-of-y-while-creating-a-word-document-using-ap
using NPOI.XWPF.Model;
using NPOI.XWPF.UserModel;

using (XWPFDocument doc = new XWPFDocument())
{
    // the body content
    XWPFParagraph paragraph = doc.CreateParagraph();
    XWPFRun run = paragraph.CreateRun();
    run.SetText("The Body:");

    paragraph = doc.CreateParagraph();
    run = paragraph.CreateRun();
    run.SetText("Lorem ipsum.... page 1");

    paragraph = doc.CreateParagraph();
    run = paragraph.CreateRun();
    run.AddBreak(BreakType.PAGE);
    run.SetText("Lorem ipsum.... page 2");

    paragraph = doc.CreateParagraph();
    run = paragraph.CreateRun();
    run.AddBreak(BreakType.PAGE);
    run.SetText("Lorem ipsum.... page 3");

    // create header-footer
    XWPFHeaderFooterPolicy headerFooterPolicy = doc.GetHeaderFooterPolicy();
    if (headerFooterPolicy == null) headerFooterPolicy = doc.CreateHeaderFooterPolicy();

    // create header start
    XWPFHeader header = headerFooterPolicy.CreateHeader(XWPFHeaderFooterPolicy.DEFAULT);
    //XWPFHeader header = doc.CreateHeader(HeaderFooterType.DEFAULT);

    paragraph = header.GetParagraphArray(0);
    if (paragraph == null) paragraph = header.CreateParagraph();
    paragraph.Alignment = ParagraphAlignment.LEFT;

    run = paragraph.CreateRun();
    run.SetText("The Header:");

    // create footer start
    XWPFFooter footer = headerFooterPolicy.CreateFooter(XWPFHeaderFooterPolicy.DEFAULT);
    //XWPFFooter footer = doc.CreateFooter(HeaderFooterType.DEFAULT);

    paragraph = footer.GetParagraphArray(0);
    if (paragraph == null) paragraph = footer.CreateParagraph();
    paragraph.Alignment = ParagraphAlignment.CENTER;

    run = paragraph.CreateRun();
    run.SetText("Page ");
    paragraph.GetCTP().AddNewFldSimple().instr = "PAGE \\* MERGEFORMAT";
    run = paragraph.CreateRun();
    run.SetText(" of ");
    paragraph.GetCTP().AddNewFldSimple().instr = "NUMPAGES \\* MERGEFORMAT";

    using (FileStream fs = new FileStream("pagenumber.docx", FileMode.Create))
    {
        doc.Write(fs);
    }
}