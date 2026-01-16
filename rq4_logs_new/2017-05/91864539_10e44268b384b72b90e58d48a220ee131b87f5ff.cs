using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using QL_BanHang.Object;
namespace QL_BanHang.Model
{
    class NguoiQuanLyMod
    {
        ConnectToSql con = new ConnectToSql();
        SqlCommand cmd = new SqlCommand();

        public DataTable GetData()
        {
            DataTable dt = new DataTable();
            cmd.CommandText = "select * from NguoiQuanLy";
            cmd.CommandType = CommandType.Text;
            cmd.Connection = con.strConn;
            try
            {
                con.OpenConnect();
                SqlDataAdapter sda = new SqlDataAdapter(cmd);
                sda.Fill(dt);
                con.CloseConnection();
            }
            catch (Exception ex)
            {
                string mes = ex.Message;
                cmd.Dispose();
                con.CloseConnection();
            }
            return dt;
        }

        public bool AddNguoiQuanLy(NguoiQuanLyObj nqlObj)
        {
            cmd.CommandText = "Insert into NguoiQuanLy values('" + nqlObj.MaNQL1 + "',N'" + nqlObj.TenNQL1 + "',CONVERT(date,'" + nqlObj.NS1.ToShortDateString() + "',103)  ,N'" + nqlObj.GT1 + "',N'" + nqlObj.DiaChi1 + "',CONVERT(date,'" + nqlObj.NgayNC1.ToShortDateString() + "',103)  ,'" + nqlObj.SDT1 + "')";
            cmd.CommandType = CommandType.Text;
            cmd.Connection = con.strConn;
            try
            {
                con.OpenConnect();
                cmd.ExecuteNonQuery();
                con.CloseConnection();
                return true;
            }
            catch (Exception ex)
            {
                string mes = ex.Message;
                cmd.Dispose();
                con.CloseConnection();
            }
            return false;
        }

        public bool DeleteNguoiQuanLy(string ma)
        {
            cmd.CommandText = "Delete NguoiQuanLy where MaNQL='" + ma + "'";
            cmd.CommandType = CommandType.Text;
            cmd.Connection = con.strConn;
            try
            {
                con.OpenConnect();
                cmd.ExecuteNonQuery();
                con.CloseConnection();
                return true;
            }
            catch (Exception ex)
            {
                string mes = ex.Message;
                cmd.Dispose();
                con.CloseConnection();
            }
            return false;
        }

        public bool UpdateNguoiQuanLy(NguoiQuanLyObj nqlObj)
        {
            cmd.CommandText = "Update NguoiQuanLy set TenNQL=N'" + nqlObj.TenNQL1 + "' ,NS=CONVERT(date,'" + nqlObj.NS1.ToShortDateString() + "',103)  ,GT =N'" + nqlObj.GT1 + "',DiaChi=N'" + nqlObj.DiaChi1 + "',NgayNC=CONVERT(date,'" + nqlObj.NgayNC1.ToShortDateString() + "',103)  ,SDT='" + nqlObj.SDT1 + "' where MaNV='" + nqlObj.MaNQL1 + "'";
            cmd.CommandType = CommandType.Text;
            cmd.Connection = con.strConn;
            try
            {
                con.OpenConnect();
                cmd.ExecuteNonQuery();
                con.CloseConnection();
                return true;
            }
            catch (Exception ex)
            {
                string mes = ex.Message;
                cmd.Dispose();
                con.CloseConnection();
            }
            return false;
        }
    }
}