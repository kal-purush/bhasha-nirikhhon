using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using ClassLibrary;
public partial class DeleteOrder : System.Web.UI.Page
{
    Int32 OrderNo;
    protected void Page_Load(object sender, EventArgs e)
    {
        OrderNo = Convert.ToInt32(Session["OrderNo"]);
    }

    protected void btnYes_Click(object sender, EventArgs e)
    {
        clsOrderCollection Order = new clsOrderCollection();
        Order.ThisOrder.Find(OrderNo);
        Order.Delete();
        Response.Redirect("OrdersList.aspx");
    }
}