using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;

using System.Data;
using System.Configuration;


public partial class _Default : System.Web.UI.Page
{
    protected void Page_Load(object sender, EventArgs e)
    {
    }

    protected void Button1_Click(object sender, EventArgs e)
    {
        string UrlService = "PageEffect.aspx";
        UrlService += "?";
        UrlService += "ServiceName=";
        UrlService += Server.UrlEncode(TextBox1.Text);
        UrlService += "&";
        UrlService += "Time=";
        UrlService += Server.UrlEncode(TextBox2.Text);
        Response.Redirect(UrlService);

    }
}