using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Drawing;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;

namespace EcommAssignment2
{
    public partial class ProfilePageCompletion : System.Web.UI.Page
    {
        String idString = "";
        String username = "";
        String password = "";
        //String source = @"Data Source=(LocalDB)\MSSQLLocalDB;AttachDbFilename=F:\Semester5\Ecommerce\EcommAssignment2\EcommAssignment2\App_Data\dragonball_database.mdf;Integrated Security=True";
        String source = @"Data Source=(LocalDB)\MSSQLLocalDB;AttachDbFilename=C:\Users\Sixta\Desktop\EcommAssignment2\EcommAssignment2\App_Data\dragonball_database.mdf;Integrated Security=True";

        protected void Page_Load(object sender, EventArgs e)
        {
            idString = Session["idString"].ToString();
            username = Request.QueryString["user"].ToString();
            password = Request.QueryString["pass"].ToString();
            update(username, password);
        }

        public void update(String username, String password)
        {
            SqlConnection connection = new SqlConnection(source);
            connection.Open();
            SqlCommand command = connection.CreateCommand();
            command.CommandType = System.Data.CommandType.Text;
            command.CommandText = "UPDATE client_table SET username = '" + username + "', password = '" + password + "' WHERE client_id = " + idString;
            command.ExecuteNonQuery();
            connection.Close();
        }
    }
}