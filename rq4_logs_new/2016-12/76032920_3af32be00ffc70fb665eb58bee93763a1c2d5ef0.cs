using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Data;
using DTO;
namespace DAL
{
    public class ChiTietXe_DAL
    {
        SqlConnection cnn = ConnectDB.ConnectData();
        public DataTable getChiTietXe(string sql)
        {
            ConnectDB.Open(cnn);
            try
            {
                SqlCommand cmd = new SqlCommand(sql, cnn);
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
                ConnectDB.Close(cnn);
            }
        }

        public Boolean Them_CTX(ChiTietXe_DTO ct)
        {
            return new ExecuteDB().ExecuteData("INSERT INTO ChiTietXe values (N'"+ct.IDXe+"', N'"+ct.HangSX+"', N'"+ct.LoaiXe+"', N'"+ct.GiaXe+"', N'"+ct.GhiChu+"', N'"+ct.TinhTrang+"')");
        }
        public Boolean Xoa_CTX(ChiTietXe_DTO ct)
        {
            return new ExecuteDB().ExecuteData("DELETE FROM ChiTietXe WHERE IDXe = N'" + ct.IDXe + "'");
        }
        public Boolean CapNhat_CTX(ChiTietXe_DTO ct)
        {
            return new ExecuteDB().ExecuteData("update ChiTietXe set HangSX = N'" +ct.HangSX+"',LoaiXe = N'"+ct.LoaiXe+"',GiaThue = N'"+ct.GiaXe+"',GhiChu =  N'"+ct.GhiChu+"',TrangThai = '"+ct.TinhTrang+"' where IDXe = N'"+ct.IDXe+"'");
        }
    }
}