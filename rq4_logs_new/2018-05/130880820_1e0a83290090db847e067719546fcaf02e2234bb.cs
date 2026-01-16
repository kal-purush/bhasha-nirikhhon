using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using System.Windows;

namespace VDIMS
{
    public partial class sign_out : System.Web.UI.Page
    {
        protected void Page_Load(object sender, EventArgs e)
        {
            Session["Logged"] = "No";
            Session["USER_NAME"] = "";
            Session["USER_ID"] = "";
            Session["IS_ADMIN"] = "false";
        }

        protected void continueBtn_Click(object sender, EventArgs e)
        {
            Response.Redirect("~/Default.aspx");
        }

    }
}