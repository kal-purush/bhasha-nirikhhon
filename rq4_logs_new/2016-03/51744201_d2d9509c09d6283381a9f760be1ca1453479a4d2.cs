using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using System.Data.SqlClient;
using System.Data;
using System.Text.RegularExpressions;


namespace WebApplication2
{
    public partial class WebForm2 : System.Web.UI.Page
    {
        protected void Page_Load(object sender, EventArgs e)
        {

        }

        protected bool checkDetails(string email, string password)
        {
            SqlConnection connection = new SqlConnection(@"Data Source=(LocalDB)\v11.0;AttachDbFilename=C:\Users\jack\Desktop\WebApplication2\WebApplication2\WebApplication2\App_Data\Database.mdf;Integrated Security=True");
            SqlDataReader reader;
            bool valid = false;
            String[] parseID = email.Split('@');
            
            String sql ="SELECT * FROM Customer WHERE EmailAddress = @email AND Password = @password";

            connection.Open();
            SqlCommand cmd = new SqlCommand(sql,connection);
            cmd.Parameters.Add("@email", SqlDbType.VarChar);
            cmd.Parameters["@email"].Value = email;

            cmd.Parameters.Add("@password", SqlDbType.VarChar);
            cmd.Parameters["@password"].Value = password;

            reader = cmd.ExecuteReader();

            if(reader.HasRows)
            {
                valid = true;
            }
            connection.Close();
            return valid;

        }

        protected void Login(object sender, EventArgs e)
        {
            if (checkDetails(txtEmail.Text, txtPassword.Text))
                Response.Redirect("/Homepage.aspx", true);
            else
                loginSuccess.Text = "Username/Password Incorrect!";
        }
    }
}