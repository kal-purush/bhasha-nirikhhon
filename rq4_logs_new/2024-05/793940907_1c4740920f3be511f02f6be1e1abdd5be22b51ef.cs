using ClassLibrary;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;

public partial class OrderLogin : System.Web.UI.Page
{
    protected void Page_Load(object sender, EventArgs e)
    {

    }

    protected void btnLogin_Click(object sender, EventArgs e)
    {
        clsOrderUser AnUser = new clsOrderUser();
        string Username = txtUsername.Text;
        string Password = txtPassword.Text;
        Boolean Found = false;
        Username = Convert.ToString(txtUsername.Text);
        Password = Convert.ToString(txtPassword.Text);
        Found = AnUser.FindUser(Username, Password);
        if (txtUsername.Text =="")
        {
            lblError.Text = "Please enter Username";
        }
       else if(txtPassword.Text == "")
        {
            lblError.Text = "Please enter Password";

        }
        else if (Found == true) {
            Response.Redirect("OrderManagementSystemList.aspx");
        }
        else if (Found == false)
        {
            lblError.Text = "Login details are incorrect Please try again";
        }
    }
}