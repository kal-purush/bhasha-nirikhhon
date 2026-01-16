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
    class MatHangMod
    {
        ConnectToSql con = new ConnectToSql();
        SqlCommand cmd = new SqlCommand();
        public DataTable GetData()
        {
            DataTable dt = new System.Data.DataTable();
            cmd.CommandText = "select * from MatHang";
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

        public DataTable GetData(string dieukien)
        {
            DataTable dt = new System.Data.DataTable();
            cmd.CommandText = "select * from MatHang" + dieukien;
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

        public bool AddMatHang(MatHangObj mhObj)
        {
            cmd.CommandText = "Insert into MatHang values('" + mhObj.MaMH1 + "',N'" + mhObj.TenMH1 + "','" + mhObj.MaNCC1 + "','" + mhObj.MaKho1 + "','" + mhObj.DonGia1 + "','" + mhObj.MaQH1 + "')";
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

        public bool DeleteMatHang(string ma)
        {
            cmd.CommandText = "xoa_MatHang'" + ma + "'";
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

        public bool UpdateMatHang(MatHangObj mhObj)
        {
            cmd.CommandText = "Update MatHang set TenMH=N'" + mhObj.TenMH1 + "',MaNCC =N'" + mhObj.MaNCC1 + "',MaKho=N'" + mhObj.MaKho1 + "',DonGia='" + mhObj.DonGia1 + "',MaQH='" + mhObj.MaQH1 + "' where MaMH='" + mhObj.MaMH1 + "'";
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