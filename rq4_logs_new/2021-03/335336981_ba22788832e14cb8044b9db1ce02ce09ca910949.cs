using ClassLibrary;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;

public partial class CustomerDelete : System.Web.UI.Page
{   //var to store the primary key value of the record deleted
    Int32 CustomerID;
    protected void Page_Load(object sender, EventArgs e)
    {
        //get the ID of the customer to be deleted from the session object
        CustomerID = Convert.ToInt32(Session["CustomerID"]);
    }

    protected void btnYes_Click(object sender, EventArgs e)
    {
        //create a new instance of the customer list
        clsCustomerCollection CustomerList = new clsCustomerCollection();
        //find the record to delete
        CustomerList.ThisCustomer.Find(CustomerID);
        //delete the record
        CustomerList.Delete();
        //redirect back to the main page
        Response.Redirect("CustomerList.aspx");
    }

    protected void Button2_Click(object sender, EventArgs e)
    {

    }
}