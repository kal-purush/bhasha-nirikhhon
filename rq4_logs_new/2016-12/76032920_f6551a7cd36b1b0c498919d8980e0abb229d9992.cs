using DTO;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DAL
{
    public class CoSoVatChat_DAL
    {
        public DataTable GetDataCSVC()
        {
            SqlConnection conn = ConnectDB.ConnectData();
            ConnectDB.Open(conn);
            try
            {
                string sql = "SELECT * FROM CoSoVatChat";
                SqlCommand cmd = new SqlCommand(sql, conn);
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
                conn.Dispose();
            }
        }

        public Boolean ThemCSVC(CoSoVatChat_DTO csvc)
        {
            string sql = "INSERT INTO CoSoVatChat VALUES(N'" + csvc.IDCSVC + "', N'" + csvc.TenCSVC + "', N'" + csvc.MaLP
                + "', '" + csvc.SoLuong + "', N'" + csvc.TinhTrang + "', N'" + csvc.GhiChu + "')";
            return new ExecuteDB().ExecuteData(sql);
        }

        public Boolean SuaCSVC(CoSoVatChat_DTO csvc)
        {
            string sql = "UPDATE CoSoVatChat SET TenCSVC = N'" + csvc.TenCSVC + "', MaLP = N'" + csvc.MaLP
                + "', SoLuong = '" + csvc.SoLuong + "', TinhTrang = '" + csvc.TinhTrang + "', GhiChu = N'" + csvc.GhiChu
                + "'WHERE IDCSVC = N'" + csvc.IDCSVC + "'";
            return new ExecuteDB().ExecuteData(sql);
        }

        public Boolean XoaCSVC(string idCSVC)
        {
            string sql = "DELETE FROM CoSoVatChat WHERE IDCSVC = N'" + idCSVC + "'";
            return new ExecuteDB().ExecuteData(sql);
        }
    }
}