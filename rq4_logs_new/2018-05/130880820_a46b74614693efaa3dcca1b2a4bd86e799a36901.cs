using System;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using System.Data;
using MySql;
using MySql.Data;
using MySql.Data.MySqlClient;

namespace VDIMS
{

    public partial class contact : System.Web.UI.Page
    {
        private String cString = "ADD ME";
        protected void Page_Load(object sender, EventArgs e)
        {
            try
            {
                if (!IsPostBack)
                {

                    MySqlConnection conn = new MySqlConnection(cString);
                    DataSet ds = new DataSet();
                    var sql = "SELECT DEALERSHIP_ID FROM DEALERSHIP ORDER BY DEALERSHIP_ID";
                    MySqlCommand cmd = new MySqlCommand(sql, conn);
                    using (MySqlDataAdapter da = new MySqlDataAdapter(cmd))
                        da.Fill(ds, "DEALERSHIP");
                    DIDdrop.DataSource = ds.Tables["DEALERSHIP"];
                    DIDdrop.DataValueField = "DEALERSHIP_ID";
                    DIDdrop.DataTextField = "DEALERSHIP_ID";
                    DIDdrop.DataBind();
                    conn.Close();
                    DIDdrop.Items.Insert(0, new ListItem("Select Dealer ID", "0"));
                }
            }
            catch (MySqlException ex)
            {
                msgTxt.ForeColor = System.Drawing.Color.Red;
                msgTxt.Text = "An error has occurred:" + "<br />" + ex.ToString();
            }
        }

        protected void DIDdrop_SelectedIndexChanged(object sender, EventArgs e)
        {
            try
            {
                string sql = "SELECT * FROM DEALERSHIP WHERE DEALERSHIP_ID = " + DIDdrop.SelectedItem.ToString();
                MySqlConnection conn = new MySqlConnection(cString);
                MySqlCommand cmd = new MySqlCommand(sql, conn);
                MySqlDataReader reader = null;
                conn.Open();
                reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    dName.Text = reader["DEALERSHIP_NAME"].ToString();
                    dAdd.Text = reader["DEALERSHIP_ADDRESS"].ToString();
                    dCity.Text = reader["DEALERSHIP_CITY"].ToString();
                    dState.Text = reader["DEALERSHIP_STATE"].ToString();
                    dZip.Text = reader["DEALERSHIP_ZIP_CODE"].ToString();
                    dPhone.Text = reader["DEALERSHIP_PHONE_NUMBER"].ToString();
                    dCars.Text = reader["NUMBER_OF_VEHICLES"].ToString();
                    dmName.Text = reader["DEALERSHIP_MANAGER_NAME"].ToString();
                    dmEmail.Text = reader["DEALERSHIP_MANAGER_EMAIL"].ToString();
                    dmPhone.Text = reader["DEALERSHIP_MANAGER_PHONE"].ToString();
                }
                reader.Close();
                reader.Dispose();
                conn.Close();
                conn.Dispose();
                cmd.Dispose();
            }
            catch (MySqlException)
            {
                Response.Redirect("contact.aspx");
            }
        }

        protected void backBtn_Click(object sender, EventArgs e)
        {
            Response.Redirect("Default.aspx");
        }
    }
}
