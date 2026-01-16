using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace A4_Net
{
    internal class Modify
    {
        public Modify() { }

        SqlDataAdapter dataAdapter;
        SqlCommand sqlCommand;

        public DataTable Table(string truyvan)
        {
            DataTable datatable = new DataTable();

            using (SqlConnection sqlConnection = connection.GetSqlConnection())
            {
                sqlConnection.Open();
                dataAdapter = new SqlDataAdapter(truyvan, sqlConnection);
                dataAdapter.Fill(datatable);
                sqlConnection.Close();
            }

            return datatable;
        }



        public void command(string truyvan)
        {
            using (SqlConnection sqlConnection = connection.GetSqlConnection())
            {
                sqlConnection.Open();
                sqlCommand = new SqlCommand(truyvan, sqlConnection);
                sqlCommand.ExecuteNonQuery();
                sqlConnection.Close();
            }
        }
    }
}