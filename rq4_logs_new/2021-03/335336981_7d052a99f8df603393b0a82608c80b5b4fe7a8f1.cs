using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using ClassLibrary;

public partial class StaffDelete : System.Web.UI.Page
{
    //var to store the primary key value of the record to be deleted
    Int32 StaffNo;

    //event handler for the load event
    protected void Page_Load(object sender, EventArgs e)
    {
        //get the number of the address to be deleted from the session object
        StaffNo = Convert.ToInt32(Session["StaffNo"]);
    }

    protected void btnYes_Click(object sender, EventArgs e)
    {
        //create a new instance of the staff list
        clsStaffCollection StaffList = new clsStaffCollection();
        //find the record to delete
        StaffList.ThisStaffMember.Find(StaffNo);
        //delete the record
        StaffList.Delete();
        //redirect to the main page
        Response.Redirect("StaffList.aspx");
    }
}