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

    public partial class delete_vehicle : System.Web.UI.Page
    {
        protected void Page_Load(object sender, EventArgs e)
        {
            if (!IsPostBack)
            {

                MySqlConnection conn = new MySqlConnection("ADD ME");
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

            }


        }

        protected void IMNdrop_SelectedIndexChanged(object sender, EventArgs e)
        {

            string connection = "ADD ME";
            string sql = "select * FROM VEHICLE WHERE IMN = " + IMNdrop.SelectedItem.ToString();
            MySqlConnection conn = new MySqlConnection(connection);
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


        }

        protected void deleteButton_Click(object sender, EventArgs e)
        {
            MySqlConnection conn = new MySqlConnection("ADD ME");
            conn.Open();
            MySqlCommand comm = conn.CreateCommand();
            MySqlCommand comm2 = conn.CreateCommand();
            String imn = IMNdrop.SelectedValue;
            var sql2 = "DELETE FROM FAVORITES WHERE IMN = " + imn;
            var sql = "DELETE FROM VEHICLE WHERE IMN = " + imn;
            comm.CommandText = sql;
            comm2.CommandText = sql2;
            comm2.ExecuteNonQuery();
            comm.ExecuteNonQuery();
            conn.Close();
            msgTxt.Text = "Vehicle successfully deleted from the database";
        }
    }
}
