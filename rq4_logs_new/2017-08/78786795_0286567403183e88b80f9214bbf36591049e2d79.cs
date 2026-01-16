using FDM90.Singleton;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Security;
using System.Web.UI;
using System.Web.UI.WebControls;

namespace FDM90.Pages.Account
{
    public partial class Logout : System.Web.UI.Page
    {
        protected void Page_Load(object sender, EventArgs e)
        {
            UserSingleton.Instance.CurrentUser = null;
            FormsAuthentication.SignOut();
            System.Threading.Thread.Sleep(3 * 1000);
            Response.Redirect("~/Pages/Content/Home.aspx");
        }
    }
}