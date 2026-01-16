using ClassLibrary;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;

public partial class OrdersLogin : System.Web.UI.Page
{
    protected void Page_Load(object sender, EventArgs e)
    {

    }

    protected void btnLogin_Click(object sender, EventArgs e)
    {
        //create an instance of the orders user class
        clsOrdersUser AUser = new clsOrdersUser();
        //create the variables to store the username and password 
        string UserName = txtUserName.Text;
        string Password = txtPassword.Text;
        //create a variable to store  the result of the find user operation
        Boolean Found = false;
        //get the username entered by the user
        UserName = Convert.ToString(txtUserName.Text);
        //get the password entered by the user
        Password = Convert.ToString(txtPassword.Text);
        //find the record
        Found = AUser.FindUser(UserName, Password);
        //if username and/or password is empty 
        if (txtUserName.Text == "")
        {
            //record the error
            lblError.Text = "Enter a Username ";
        }
        else if (txtPassword.Text == "")
        {
            //record the error
            lblError.Text = "Enter a Password ";
        }
        //if found 
        else if (Found == true)
        {
            //redirect to the list page
            Response.Redirect("OrdersList.aspx");
        }
        else if (Found == false )
        {
            //record the error
            lblError.Text = "Login details are incorrect. Please try again ";

        }
    }
}