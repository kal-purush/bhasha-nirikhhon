using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;

namespace QL_BanHang.Model
{
    class ConnectToSql
    {

        #region Availible
        private SqlConnection Conn;
        private SqlCommand cmd;
        public SqlCommand CMD
        {
            get { return cmd; }
            set { cmd = value; }
        }
        //  public SqlConnection Connection { get { return Conn; } }
        private string error;
        public string Erro
        {
            get { return error; }
            set { error = value; }
        }
        // public string StrCon;
        #endregion

        #region contruction
        public void ConnectToSQL()
        {
            Conn = new SqlConnection(@"Data Source=DESKTOP-P8Q0593\SQLEXPRESS;Initial Catalog=quản lý bán hàng siêu thị;Integrated Security=True");
        }
        #endregion
        #region method

        public void OpenConnect()
        {
            try
            {
                if (Conn.State == ConnectionState.Closed)
                {
                    Conn.Open();
                }
            }
            catch (Exception ex)
            {
                error = ex.Message;
                // return false;
            }
            //  return true;
        }
        public void CloseConnection()
        {
            try
            {
                if (Conn.State == ConnectionState.Open)
                {
                    Conn.Close();
                }
            }
            catch (Exception ex)
            {
                error = ex.Message;
                // return false;
            }
            // return true;
        }
        public SqlConnection strConn
        {
            get { return Conn; }
        }
        #endregion
    }
}
