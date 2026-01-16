using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Security;
using System.Web.UI;
using System.Web.UI.WebControls;
using System.Web.UI.WebControls.WebParts;
using System.Web.UI.HtmlControls;
using BusinessObject;
using Entities;


public partial class Login : System.Web.UI.Page
{
    protected void Page_Load(object sender, EventArgs e)
    {

    }

    protected void btnIngresar_Click(object sender, System.EventArgs e)
    {
        Response.Redirect("Modificaciones.aspx");
    }

    protected void btnCancelar_Click(object sender, System.EventArgs e)
    {
        Response.Redirect("PaginaInicio.aspx");
    }


}


