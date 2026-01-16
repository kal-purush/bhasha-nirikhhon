using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace QLHocBongMLV
{
     class DataServices
    {
        private static SqlConnection mySqlConnection;
        private SqlDataAdapter myDataAdapter;
        public bool OpenDB()
        {
            string conStr ="Data Source = ITPhan; Initial Catalog = HOCBONG_MLV; Integrated Security = True";
            try
            {
                mySqlConnection = new SqlConnection(conStr);
                mySqlConnection.Open();
            }
            catch (SqlException ex)
            {
                MessageBox.Show(ex.Message, "Error " + ex.Number.ToString());
                mySqlConnection = null;
                return false;
            }
            return true;
        }
        public DataTable RunQuery(string sSql)
        {
            DataTable myDataTable = new DataTable();
            try
            {
                OpenDB();
                myDataAdapter = new SqlDataAdapter(sSql, mySqlConnection);
                SqlCommandBuilder mySqlCommandBuilder = new SqlCommandBuilder(myDataAdapter);
                myDataAdapter.Fill(myDataTable);
            }
            catch (SqlException ex)
            {
                MessageBox.Show(ex.Message, "Error " + ex.Number.ToString());
                return null;
            }
            return myDataTable;
        }
        public void Update(DataTable myDataTable)
        {
            try
            {
                myDataAdapter.Update(myDataTable);
            }
            catch (SqlException ex)
            {
                MessageBox.Show(ex.Message, "Error " + ex.Number.ToString());
            }
        }
        public void ExecuteNonQuery(string sSql)
        {
            SqlCommand mySqlCommand = new SqlCommand(sSql, mySqlConnection);
            try
            {
                mySqlCommand.ExecuteNonQuery();
            }
            catch (SqlException ex)
            {
                MessageBox.Show(ex.Message, "Error " + ex.Number.ToString());
            }
        }
    }
}