using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using FlightClasses;


public partial class ACustomer : System.Web.UI.Page
{
    protected void Page_Load(object sender, EventArgs e)
    {

    }



    protected void btnOK_Click(object sender, EventArgs e)
    {
        //create a new instance of clsCustomer
        clsCustomer ACustomer = new clsCustomer();
        //capture the Name
        ACustomer.Name = txtName.Text;
        //capture the Phone
        ACustomer.Phone = txtPhone.Text;
        //capture the email
        ACustomer.Email = txtEmail.Text;
        //capture the username
        ACustomer.UserName = txtUserName.Text;
        //capture the password
        ACustomer.Password = txtPassword.Text;
        //capture the paymentid
        ACustomer.PaymentID = txtPaymentID.Text;
        //capture the payment type
        ACustomer.PaymentType = txtPaymentType.Text;
        //store the customer in the session object
        Session["ACustomer"] = ACustomer;
        //redirect to the viewer page
        Response.Redirect("CustomerViewer.aspx");
    }
}