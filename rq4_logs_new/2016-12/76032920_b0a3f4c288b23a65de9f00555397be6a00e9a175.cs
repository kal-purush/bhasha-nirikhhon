using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DTO;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;
namespace DAL
{
    public class ChiTietTP_DAL
    {
        SqlConnection conn = ConnectDB.ConnectionDB();
        public DataTable GetDataCTTP()
        {
            ConnectDB.Open(conn);
            try
            {
                SqlCommand cmd = new SqlCommand("SELECT * FROM ChiTietThucPham", conn);
                SqlDataAdapter da = new SqlDataAdapter(cmd);
                cmd.Dispose();
                DataTable dt = new DataTable();
                da.Fill(dt);
                return dt;
            }
            catch (Exception)
            {
                return null;
            }
            finally
            {
                ConnectDB.Close(conn);
            }
        }
        private Boolean ExecuteDataCTTP(string sql)
        {
            ConnectDB.Open(conn);
            try
            {
                SqlCommand cmd = new SqlCommand(sql, conn);
                cmd.ExecuteNonQuery();
                cmd.Dispose();
                return true;
            }
            catch (Exception)
            {
                return false;
            }
            finally
            {
                ConnectDB.Close(conn);
            }
        }
        public Boolean ThemCTTP(ChiTietThucPham_DTO TP)
        {
            ConnectDB.Open(conn);
            try
            {
                string sql = "INSERT INTO ChiTietThucPham(IDTP, TenTP, NhaSX, NgaySX, HanSD, MieuTa) VALUES(@idTP, @tenTP, @nhaSX, @ngaySX, @hanSD, @mieuTa)";
                SqlCommand cmd = new SqlCommand(sql, conn);
                if (TP.IDTP == "")
                {
                    throw new Exception("Field \"IDTP\" không thể để trống!");
                }
                cmd.Parameters.AddWithValue("@idTP", TP.IDTP);
                cmd.Parameters.AddWithValue("@tenTP", TP.TenTP);
                cmd.Parameters.AddWithValue("@nhaSX", TP.NhaSX);
                cmd.Parameters.AddWithValue("@ngaySX", TP.NgaySX);
                cmd.Parameters.AddWithValue("@hanSD", TP.HanSD);
                cmd.Parameters.AddWithValue("@mieuTa", TP.MieuTa);
                cmd.ExecuteNonQuery();
                cmd.Dispose();
                return true;
            }
            catch (Exception)
            {
                return false;
            }
            finally
            {
                ConnectDB.Close(conn);
            }
        }
        public Boolean SuaCTTP(ChiTietThucPham_DTO TP)
        {
            ConnectDB.Open(conn);
            try
            {
                return ExecuteDataCTTP("UPDATE ChiTietThucPham SET TenTP = N'" + TP.TenTP
                    + "', NhaSX = N'" + TP.NhaSX + "', NgaySX = N'" + TP.NgaySX + "', HanSD = '" + TP.HanSD
                    + "', MieuTa = N'" + TP.MieuTa + "'WHERE IDTP = N'" + TP.IDTP + "'");
            }
            catch (Exception)
            {             
                return false;
            }
            finally
            {
                ConnectDB.Close(conn);
            }
        }
        public Boolean XoaCTTP(string idTP)
        {
            ConnectDB.Open(conn);
            try
            {
                return ExecuteDataCTTP("DELETE FROM ChiTietThucPham WHERE IDTP = N'" + idTP + "'");
            }
            catch (Exception)
            {
                return false;
            }
            finally
            {
                ConnectDB.Close(conn);
            }
        }
    }

}