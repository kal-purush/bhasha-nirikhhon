using System;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using System.Data;
using MySql;
using MySql.Data;
using MySql.Data.MySqlClient;

namespace VDIMS.admin
{

    public partial class sell_vehicle : System.Web.UI.Page
    {
        private String cString = "ADD ME";
        protected void Page_Load(object sender, EventArgs e)
        {
            if (!IsPostBack)
            {
                try
                {
                    MySqlConnection conn = new MySqlConnection(cString);
                    DataSet ds = new DataSet();
                    var sql = "SELECT IMN FROM VEHICLE";
                    MySqlCommand cmd = new MySqlCommand(sql, conn);
                    using (MySqlDataAdapter da = new MySqlDataAdapter(cmd))
                        da.Fill(ds, "VEHICLE");
                    IMNdrop.DataSource = ds.Tables["VEHICLE"];
                    IMNdrop.DataValueField = "IMN";
                    IMNdrop.DataTextField = "IMN";
                    IMNdrop.DataBind();
                    conn.Close();
                    IMNdrop.Items.Insert(0, new ListItem("Select IMN", "0"));
                } catch(MySqlException ex)
                {
                    msgTxt.ForeColor = System.Drawing.Color.Red;
                    msgTxt.Text = "An error has occurred:" + "<br />" + ex.ToString();
                }
            }
        }

        protected void IMNdrop_SelectedIndexChanged(object sender, EventArgs e)
        {
            try
            {
                string sql = "select * FROM VEHICLE WHERE IMN = " + IMNdrop.SelectedItem.ToString();
                MySqlConnection conn = new MySqlConnection(cString);
                MySqlCommand cmd = new MySqlCommand(sql, conn);
                MySqlDataReader reader = null;
                conn.Open();
                reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    make.Text = reader["MAKE"].ToString();
                    model.Text = reader["MODEL"].ToString();
                    year.Text = reader["VEHICLE_YEAR"].ToString();
                    extColor.Text = reader["EXTERIOR_COLOR"].ToString();
                    intColor.Text = reader["INTERIOR_COLOR"].ToString();
                    Mileage.Text = reader["MILEAGE"].ToString();
                    cMpg.Text = reader["CITY_MPG"].ToString();
                    hMpg.Text = reader["HIGHWAY_MPG"].ToString();
                    engine.Text = reader["VEHICLE_ENGINE"].ToString();
                    transmission.Text = reader["TRANSMISSION"].ToString();
                    price.Text = reader["PRICE"].ToString();
                    vin.Text = reader["VIN"].ToString();
                    String cond = reader["VEHICLE_CONDITION"].ToString();
                    foreach (ListItem i in condition.Items)
                    {
                        if (i.Value == cond)
                        {
                            condition.SelectedIndex = condition.Items.IndexOf(condition.Items.FindByValue(cond));
                        }
                    }
                    location.SelectedIndex = Convert.ToInt32(reader["DEALERSHIP_ID"].ToString()) - 1;
                }
                reader.Close();
                reader.Dispose();
                conn.Close();
                conn.Dispose();
                cmd.Dispose();
            } catch(MySqlException)
            {
                Response.Redirect("sell_vehicle.aspx");
            }
        }

        protected void soldButton_Click(object sender, EventArgs e)
        {
            try
            {
                MySqlConnection conn = new MySqlConnection(cString);
                conn.Open();
                MySqlCommand comm = conn.CreateCommand();
                MySqlCommand comm2 = conn.CreateCommand();
                MySqlCommand comm3 = conn.CreateCommand();
                String imn = IMNdrop.SelectedValue;
                var sql2 = "DELETE FROM FAVORITES WHERE IMN = " + imn;
                var sql3 = "INSERT INTO SOLD_VEHICLES SELECT v.* FROM VEHICLE v WHERE IMN = " + imn;
                var sql = "DELETE FROM VEHICLE WHERE IMN = " + imn;
                comm.CommandText = sql;
                comm2.CommandText = sql2;
                comm3.CommandText = sql3;
                comm2.ExecuteNonQuery();
                comm3.ExecuteNonQuery();
                comm.ExecuteNonQuery();
                conn.Close();
                msgTxt.ForeColor = System.Drawing.Color.Empty;
                msgTxt.Text = "Vehicle successfully marked as sold";
            } catch(MySqlException ex)
            {
                msgTxt.ForeColor = System.Drawing.Color.Red;
                msgTxt.Text = "Vehicle could not be marked as sold; refer to the below error message:" + "<br />" + ex.ToString();
            }
        }

        protected void backBtn_Click(object sender, EventArgs e)
        {
            Response.Redirect("/admin/inventory.aspx");

        }
    }
}
