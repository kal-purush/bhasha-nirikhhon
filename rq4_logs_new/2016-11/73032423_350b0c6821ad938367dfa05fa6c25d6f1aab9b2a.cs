using System;
using System.Collections;
using System.Configuration;
using System.Data;
using System.Linq;
using System.Web;
using System.Web.Security;
using System.Web.UI;
using System.Web.UI.HtmlControls;
using System.Web.UI.WebControls;
using System.Web.UI.WebControls.WebParts;
using System.Xml.Linq;
using System.Drawing;

//这是一个生成验证码的网页
public partial class CheckCode : System.Web.UI.Page
{
    private string GenerateCheckCode()
    {
        int num;
        char code;
        string checkCode = string.Empty;
        Random random = new Random();
        for (int i = 0; i < 6; i++)//循环次数决定验证码的位数
        {
            num = random.Next();
            if (num % 2 == 0)
            {
                code = (char)('0' + (char)(num % 10));
            }
            else
            {
                code = (char)('a' + (char)(num % 26));
            }

            checkCode += code.ToString();
        }
        Response.Cookies.Add(new HttpCookie("CheckCode", checkCode));
        return checkCode;
    }

    protected void Page_Load(object sender, EventArgs e)
    {
        CreateCheckCodeImg(GenerateCheckCode());
    }

    private void CreateCheckCodeImg(string checkCode)
    {
        if (checkCode == null || checkCode.Trim() == String.Empty)
            return;
        Bitmap img = new Bitmap((int)Math.Ceiling((checkCode.Length * 12.5)), 22);

        Graphics g = Graphics.FromImage(img);
        try
        {
            Random random = new Random();
            g.Clear(Color.White);


            //画图片的背景线
            for (int i = 0; i < 2; i++)
            {
                int x1 = random.Next(img.Width);
                int x2 = random.Next(img.Width);
                int y1 = random.Next(img.Width);
                int y2 = random.Next(img.Width);
                g.DrawLine(new Pen(Color.Black), x1, y1, x2, y2);
            }

            //画出指定的字符
            Font font = new Font("Arial", 12, (FontStyle.Bold));
            System.Drawing.Drawing2D.LinearGradientBrush brush = new System.Drawing.Drawing2D.LinearGradientBrush(new Rectangle(0, 0, img.Width, img.Height), Color.Blue, Color.Red, 1.2f, true);
            g.DrawString(checkCode, font, brush, 2, 2);

            //画图片的前景噪点
            for (int i = 0; i < 100; i++)
            {
                int x = random.Next(img.Width);
                int y = random.Next(img.Height);
                img.SetPixel(x, y, Color.FromArgb(random.Next()));
            }
            g.DrawRectangle(new Pen(Color.Silver), 0, 0, img.Width - 1, img.Height - 1);
            System.IO.MemoryStream ms = new System.IO.MemoryStream();
            img.Save(ms, System.Drawing.Imaging.ImageFormat.Gif);
            Response.ClearContent();
            Response.ContentType = "image/Gif";
            Response.BinaryWrite(ms.ToArray());

        }
        finally
        {
            g.Dispose();
            img.Dispose();
        }
    }
}