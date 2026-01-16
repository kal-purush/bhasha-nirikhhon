using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using System.Data;
using System.Data.SqlClient;
using System.Configuration;
using System.Text.RegularExpressions;
using System.Web.UI.WebControls.WebParts;
using System.Web.UI.HtmlControls;
using System.Web.Configuration;
using System.Text;

namespace WebApplication2
{
    public partial class OrderView : System.Web.UI.Page
    {
        protected void Page_Load(object sender, EventArgs e)
        {
            try
            {
                string strSQLconnection = @"Data Source=(LocalDB)\v11.0;AttachDbFilename=C:\Users\werl\Documents\Visual Studio 2013\Projects\SystemsAnalysis\WebApplication2\WebApplication2\App_Data\Database.mdf;Integrated Security=True";
                SqlConnection sqlConnection = new SqlConnection(strSQLconnection);
                SqlCommand sqlCommand = new SqlCommand("SELECT * FROM [Order] WHERE OrderID = (SELECT MAX(OrderID) FROM [Order])", sqlConnection);
                sqlConnection.Open();

                SqlDataReader reader = sqlCommand.ExecuteReader();

                GridView1.DataSource = reader;
                GridView1.DataBind();
            }
            catch (SqlException sept)
            {
                Response.Write(sept.Message);
            }
        }
    }
}