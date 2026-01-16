using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;

namespace VDIMS.admin
{
    public partial class backend : System.Web.UI.Page
    {
        protected void Page_Load(object sender, EventArgs e)
        {
            if(Session["Logged"].Equals("Yes") && Session["IS_ADMIN"].Equals("true"))
            {

            } else
            {
                Response.Redirect("~/sign_in.aspx");
            }
            
        }
    }
}