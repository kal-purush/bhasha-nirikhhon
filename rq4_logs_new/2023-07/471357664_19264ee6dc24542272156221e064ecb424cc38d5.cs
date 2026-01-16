// See https://aka.ms/new-console-template for more information
using NPOI.XWPF.UserModel;

using (XWPFDocument doc = new XWPFDocument())
{
    var comments=doc.CreateComments();
    var comment1=comments.CreateComment("0");
    comment1.Author = "Tony";
    comment1.Initials = "S";
    comment1.Date = DateTime.Now.ToShortDateString();

    var comment2=comments.CreateComment("1");
    comment2.Author = "May";
    var para=comment2.CreateParagraph();
    var run2 = para.CreateRun();
    run2.SetText("Hello World");

    var paragraph1 = doc.CreateParagraph();
    paragraph1.CreateRun().SetText("This is ");
    paragraph1.GetCTP().AddNewCommentRangeStart().id = comment1.Id;
    paragraph1.CreateRun().SetText("1st comment");
    paragraph1.GetCTP().AddNewCommentRangeEnd().id = comment1.Id;
    paragraph1.CreateRun().SetText(" text.");
    paragraph1.GetCTP().AddNewR().AddNewCommentReference().id = comment1.Id;

    var paragraph2 = doc.CreateParagraph();
    paragraph2.CreateRun().SetText("This is ");
    paragraph2.GetCTP().AddNewCommentRangeStart().id = comment2.Id;
    paragraph2.CreateRun().SetText("2nd comment");
    paragraph2.GetCTP().AddNewCommentRangeEnd().id = comment2.Id;
    paragraph2.CreateRun().SetText(" text.");
    paragraph2.GetCTP().AddNewR().AddNewCommentReference().id = comment2.Id;


    using (FileStream sw = File.Create("comments.docx"))
    {
        doc.Write(sw);
    }
}