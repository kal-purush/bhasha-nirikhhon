using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;

namespace Fast_Food_Restaurant.DAO
{
    class DataProvider
    {
        private static DataProvider instance;

        public static DataProvider Instance
        {
            get { if (instance == null) instance = new DataProvider(); return instance; }
            private set { instance = value; }
        }
        private DataProvider() { }
        private string query = "Data Source=DESKTOP-KODMELM;Initial Catalog=NHA_HANG_AN;Integrated Security=True";
        protected SqlConnection cnn;
        protected SqlCommand cm;

        internal DataTable ExecuteReader(string query)
        {
            throw new NotImplementedException();
        }

        internal DataTable ExecuteReader(string query, object[] v)
        {
            throw new NotImplementedException();
        }

        protected SqlDataAdapter adapter;

        //protected SqlDataAdapter adapter;



        protected void connect()
        {
            cnn = new SqlConnection(query);
            cnn.Open();
        }
        protected void disconnect()
        {
            cnn.Close();
        }

        public DataTable ExecuteQuery(string sql, object[] parameter = null)
        {
            DataTable data = new DataTable();
            using (cnn = new SqlConnection(query))
            {
                connect();
                cm = new SqlCommand(sql, cnn);
                if (parameter != null)
                {
                    string[] listPara = sql.Split(' ');
                    int i = 0;
                    foreach (string item in listPara)
                    {
                        if (item.Contains('@'))
                        {
                            cm.Parameters.AddWithValue(item, parameter[i]);
                            i++;
                        }
                    }
                }
            }

            adapter = new SqlDataAdapter(cm);
            adapter.Fill(data);
            disconnect();

            return data;
        }

        public SqlDataReader ReadData(string ss, object[] parameter = null)
        {
            cnn = new SqlConnection();

            connect();
            cm = cnn.CreateCommand();
            cm.CommandText = ss;
            cm.CommandType = CommandType.Text;
            SqlDataReader reader = cm.ExecuteReader();
            return reader;
        }
        public void DisConnect()
        {
            disconnect();
        }
        public int ExecuteNonQuery(string sql, object[] parameter = null)
        {
            int data = 0;
            using (cnn = new SqlConnection(query))
            {
                connect();
                cm = new SqlCommand(sql, cnn);
                if (parameter != null)
                {
                    string[] listPara = sql.Split(' ');
                    int i = 0;
                    foreach (string item in listPara)
                    {
                        if (item.Contains('@'))
                        {
                            cm.Parameters.AddWithValue(item, parameter[i]);
                            i++;
                        }
                    }
                }

            }
            data = cm.ExecuteNonQuery();
            disconnect();
            return data;
        }
        public object ExecuteScalar(string sql, object[] parameter = null)
        {
            object data = 0;
            using (cnn = new SqlConnection(query))
            {
                connect();
                cm = new SqlCommand(sql, cnn);
                if (parameter != null)
                {
                    string[] listPara = sql.Split(' ');
                    int i = 0;
                    foreach (string item in listPara)
                    {
                        if (item.Contains('@'))
                        {
                            cm.Parameters.AddWithValue(item, parameter[i]);
                            i++;
                        }
                    }
                }

            }
            data = cm.ExecuteScalar();
            disconnect();
            return data;
        }

    }
}