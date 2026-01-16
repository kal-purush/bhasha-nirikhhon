using ClassLibrary;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;

public partial class ReceiptManagementSystemLogin : System.Web.UI.Page
{
    protected void Page_Load(object sender, EventArgs e)
    {

    }

    protected void btnLogin_Click(object sender, EventArgs e)
    {
        clsReceiptUser AnUser = new clsReceiptUser();
        string UserName = txtUsername.Text;
        string Password = txtPassword.Text;

        Boolean Found  = false;

        UserName = Convert.ToString(UserName);
        Password = Convert.ToString(Password);

        Found = AnUser.FindUser(UserName, Password);

        if(txtPassword.Text == "")
        {
            lblError.Text = "Enter a Password";
        }

        else if(txtUsername.Text == "")
        {
            lblError.Text = "Enter a username";
        }
        else if(Found == true)
        {
            Response.Redirect("ReceiptManagementSystemList.aspx");
        }
        else if(Found == false)
        {
            lblError.Text = "Login details are incorrect. Please try again";
        }
        lblError.Visible = true;
    }
}