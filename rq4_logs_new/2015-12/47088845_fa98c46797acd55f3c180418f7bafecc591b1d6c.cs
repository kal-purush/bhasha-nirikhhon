using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;

namespace QL_HS_GV.Class
{
    class Connection
    {
        private string connectionString = ConfigurationManager.ConnectionStrings["ConnectionString"].ConnectionString;
        private SqlConnection con;
        public Connection()
        {
            connectionString = ConfigurationManager.ConnectionStrings["ConnectionString"].ConnectionString;
            con = new SqlConnection(connectionString);
        }

        public bool isConnect()
        {
            bool connect = false;
            try
            {
                con.Open();
                if (con.State == ConnectionState.Open)
                {
                    connect = true;
                }
                else
                {
                    throw new Exception("Connection err!");
                }
            }
            catch (Exception e)
            {
            }
            return connect;
        }

        public DataTable LoadData()
        {
            DataTable dt = new DataTable();
            if (isConnect())
            {
                SqlCommand com = new SqlCommand();
                com.CommandText = "LoadHS";
                com.CommandType = CommandType.StoredProcedure;
                com.Connection = con;
                SqlDataAdapter ad = new SqlDataAdapter(com);
                ad.Fill(dt);
            }
            con.Close();
            return dt;
        }
        public int  Login(string ID, string Pass)
        {
            int ktra = 0;
            if (isConnect())
            {
                SqlCommand com = new SqlCommand();
                com.CommandText = "DangNhap";
                com.CommandType = CommandType.StoredProcedure;
                com.Connection = con;
                com.ExecuteNonQuery();
                SqlDataReader reader = com.ExecuteReader();
                while (reader.Read())
                {
                    if (reader[0].ToString() == ID)
                    {
                        if (reader[1].ToString() == Pass)
                        {
                            ktra = 2;
                        }
                        else
                            ktra = 1;
                        break;
                    }
                    else
                        ktra = 0;
                }
            }
            con.Close();
            return ktra;
        }
        public bool ThemHS(Hocsinh hs)
        {
            bool ktra = false;
            if (isConnect())
            {
                SqlCommand com = new SqlCommand();
                com.CommandText = "ThemDL";
                com.CommandType = CommandType.StoredProcedure;
                com.Parameters.AddWithValue("@HoTen", hs.Name1);
                com.Parameters.AddWithValue("@GT", hs.GT1);
                com.Parameters.AddWithValue("@NS", hs.NS1);
                com.Parameters.AddWithValue("@Lop", hs.Lop1);
                com.Parameters.AddWithValue("@Khoa", hs.Khoa1);
                com.Connection = con;
                int i = com.ExecuteNonQuery();
                ktra = i > 0 ? true : false;
            }
            con.Close();
            return ktra;
        }
        public bool SuaHS(Hocsinh hs)
        {
            bool ktra = false;
            if (isConnect())
            {
                SqlCommand com = new SqlCommand();
                com.CommandText = "SuaHS";
                com.CommandType = CommandType.StoredProcedure;
                com.Parameters.AddWithValue("@ID", hs.ID1);
                com.Parameters.AddWithValue("@HoTen", hs.Name1);
                com.Parameters.AddWithValue("@GT", hs.GT1);
                com.Parameters.AddWithValue("@NS", hs.NS1);
                com.Parameters.AddWithValue("@Lop", hs.Lop1);
                com.Parameters.AddWithValue("@Khoa", hs.Khoa1);
                com.Connection = con;
                int i = com.ExecuteNonQuery();
                ktra = i > 0 ? true : false;
            }
            con.Close();
            return ktra;
        }

        public bool XoaHS(string id)
        {
            bool ktra = false;
            if (isConnect())
            {
                SqlCommand com = new SqlCommand();
                com.CommandText = "XoaHS";
                com.CommandType = CommandType.StoredProcedure;
                com.Parameters.AddWithValue("@ID", id);
                com.Connection = con;
                int i = com.ExecuteNonQuery();
                ktra = i > 0 ? true : false;
            }
            con.Close();
            return ktra;
        }

        public DataTable Timkiem(string TK)
        {
            DataTable dt = new DataTable();
            if (isConnect())
            {
                SqlCommand com = new SqlCommand();
                com.CommandText = "TimKiem";
                com.CommandType = CommandType.StoredProcedure;
                com.Parameters.AddWithValue("@TK", TK);
                com.Connection = con;
                com.ExecuteNonQuery();
                SqlDataAdapter ad = new SqlDataAdapter(com);
                ad.Fill(dt);
            }
            con.Close();
            return dt;
        }
        
    }
}